#include <asio.hpp>
#include <array>
#include <iostream>
#include <vector>

#include "connection_pool.hpp"
#include "hash_ring.hpp"
#include "rate_limiter.hpp"
#include "session.hpp"

using asio::ip::tcp;

namespace {

asio::awaitable<void> send_429(tcp::socket socket) {
    static const std::string body = "Rate limit exceeded\n";
    std::string response =
        "HTTP/1.1 429 Too Many Requests\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" + body;

    asio::error_code ec;
    co_await asio::async_write(socket, asio::buffer(response),
                                asio::redirect_error(asio::use_awaitable, ec));

    // We reject before ever reading the client's request, so there may
    // still be unread bytes sitting in our receive buffer at this point.
    // Destroying the socket now would make the kernel send RST instead
    // of a clean FIN, and the client would see "connection reset by
    // peer" instead of the 429 body we just wrote. Half-close the write
    // side, then drain whatever the client already sent, before we let
    // the socket close normally.
    socket.shutdown(tcp::socket::shutdown_send, ec);
    std::array<char, 512> discard;
    for (;;) {
        std::size_t n = co_await socket.async_read_some(
            asio::buffer(discard), asio::redirect_error(asio::use_awaitable, ec));
        if (ec || n == 0) break;
    }
}

asio::awaitable<void> reap_loop(aegis::RateLimiter& limiter) {
    asio::steady_timer timer(co_await asio::this_coro::executor);
    for (;;) {
        timer.expires_after(std::chrono::seconds(60));
        co_await timer.async_wait(asio::use_awaitable);
        auto removed = limiter.reap_idle(std::chrono::minutes(5));
        if (removed > 0) {
            std::cout << "[rate-limiter] reaped " << removed << " idle client buckets ("
                      << limiter.total_tracked_clients() << " remaining)\n";
        }
    }
}

asio::awaitable<void> listen(tcp::acceptor& acceptor, aegis::ConnectionPool& pool,
                              aegis::HashRing& ring, aegis::RateLimiter& limiter) {
    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
        socket.set_option(tcp::no_delay(true));

        asio::error_code ec;
        auto remote = socket.remote_endpoint(ec);
        if (ec) {
            std::cerr << "[aegis] could not read remote endpoint, dropping connection\n";
            continue;
        }
        std::string client_key = remote.address().to_string();

        // Rate limiting happens before any routing logic - reject cheap,
        // before we spend a pool acquisition or hash ring lookup on a
        // request we're about to drop anyway.
        if (!limiter.allow(client_key)) {
            asio::co_spawn(acceptor.get_executor(), send_429(std::move(socket)), asio::detached);
            continue;
        }

        auto backend_index = ring.get_backend(client_key);
        if (!backend_index) {
            std::cerr << "[aegis] no backends registered in ring, dropping connection\n";
            continue;
        }

        asio::co_spawn(
            acceptor.get_executor(),
            aegis::handle_client(std::move(socket), pool, *backend_index),
            asio::detached);
    }
}

// Ensures the pool is warmed before we start accepting client traffic,
// without blocking the io_context thread while warm-up connections happen.
asio::awaitable<void> run(tcp::acceptor& acceptor, aegis::ConnectionPool& pool,
                           aegis::HashRing& ring, aegis::RateLimiter& limiter) {
    co_await pool.warm_up();
    asio::co_spawn(co_await asio::this_coro::executor, reap_loop(limiter), asio::detached);
    co_await listen(acceptor, pool, ring, limiter);
}

} // namespace

int main(int argc, char* argv[]) {
    unsigned short listen_port = 8080;
    if (argc > 1) {
        listen_port = static_cast<unsigned short>(std::stoi(argv[1]));
    }

    try {
        asio::io_context io_context(1); // single-threaded for Phase 2

        // Matches the mock backend cluster from Phase 1 (ports 9001-9003).
        std::vector<aegis::BackendConfig> backends = {
            {"127.0.0.1", "9001", /*min_pool_size=*/4},
            {"127.0.0.1", "9002", /*min_pool_size=*/4},
            {"127.0.0.1", "9003", /*min_pool_size=*/4},
        };

        aegis::ConnectionPool pool(io_context, backends);

        aegis::HashRing ring;
        for (std::size_t i = 0; i < backends.size(); ++i) {
            ring.add_backend(i);
        }
        std::cout << "[aegis] hash ring: " << ring.backend_count() << " backends, "
                  << ring.vnode_count() << " total vnodes\n";

        // 10 requests/sec sustained, burst up to 20 - deliberately loose
        // defaults for local testing; production values should come from
        // measured per-backend capacity, not a guess.
        aegis::RateLimiter limiter(/*refill_rate_per_sec=*/10.0, /*capacity=*/20.0);

        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), listen_port));
        std::cout << "[aegis] listening on 0.0.0.0:" << listen_port << "\n";

        asio::co_spawn(io_context, run(acceptor, pool, ring, limiter), asio::detached);

        asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        io_context.run();
    } catch (const std::exception& e) {
        std::cerr << "[aegis] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}