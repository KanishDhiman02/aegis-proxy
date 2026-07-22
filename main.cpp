#include <asio.hpp>
#include <iostream>
#include <vector>

#include "connection_pool.hpp"
#include "hash_ring.hpp"
#include "session.hpp"

using asio::ip::tcp;

namespace {

asio::awaitable<void> listen(tcp::acceptor& acceptor, aegis::ConnectionPool& pool,
                              aegis::HashRing& ring) {
    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
        socket.set_option(tcp::no_delay(true));

        // Route by client IP, not per-connection randomness, so the same
        // client keeps landing on the same backend (session affinity /
        // cache warmth) as long as that backend stays in the ring - the
        // whole point of consistent hashing over a naive round-robin.
        asio::error_code ec;
        auto remote = socket.remote_endpoint(ec);
        if (ec) {
            std::cerr << "[aegis] could not read remote endpoint, dropping connection\n";
            continue;
        }
        std::string client_key = remote.address().to_string();

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
                           aegis::HashRing& ring) {
    co_await pool.warm_up();
    co_await listen(acceptor, pool, ring);
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

        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), listen_port));
        std::cout << "[aegis] listening on 0.0.0.0:" << listen_port << "\n";

        asio::co_spawn(io_context, run(acceptor, pool, ring), asio::detached);

        asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        io_context.run();
    } catch (const std::exception& e) {
        std::cerr << "[aegis] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}