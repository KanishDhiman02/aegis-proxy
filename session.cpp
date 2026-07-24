#include "session.hpp"
#include <asio/experimental/awaitable_operators.hpp>
#include <array>
#include <iostream>
#include <optional>
#include <unordered_set>

namespace aegis {

using namespace asio::experimental::awaitable_operators;

namespace {

// Relays bytes from `from` to `to` until `from` is closed or errors.
// If `response_started` is non-null, it's set the moment the FIRST
// chunk is forwarded - this is the retry-safety signal: it records the
// fact "the client has received at least one byte", independent of
// which relay direction "wins" the operator|| race below, since the
// flag is set as a side effect during execution, not from the return
// value.
asio::awaitable<void> relay(asio::ip::tcp::socket& from, asio::ip::tcp::socket& to,
                             std::atomic<bool>* response_started) {
    std::array<char, 8192> buffer;
    asio::error_code ec;

    for (;;) {
        std::size_t n = co_await from.async_read_some(
            asio::buffer(buffer), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            co_return;
        }

        if (response_started) {
            response_started->store(true, std::memory_order_relaxed);
        }

        co_await asio::async_write(
            to, asio::buffer(buffer, n), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            co_return;
        }
    }
}

asio::awaitable<void> send_status(asio::ip::tcp::socket& socket, int code,
                                   const std::string& reason, const std::string& body) {
    std::string response =
        "HTTP/1.1 " + std::to_string(code) + " " + reason + "\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" + body;

    asio::error_code ec;
    co_await asio::async_write(socket, asio::buffer(response),
                                asio::redirect_error(asio::use_awaitable, ec));

    // Same reasoning as the rate limiter's 429 path (Phase 4): don't
    // destroy the socket with unread client bytes still sitting in the
    // receive buffer, or the kernel sends RST instead of a clean FIN.
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
    std::array<char, 512> discard;
    for (;;) {
        std::size_t n = co_await socket.async_read_some(
            asio::buffer(discard), asio::redirect_error(asio::use_awaitable, ec));
        if (ec || n == 0) break;
    }
}

} // namespace

asio::awaitable<void> handle_client(asio::ip::tcp::socket client_socket,
                                     ConnectionPool& pool,
                                     HashRing& ring,
                                     std::deque<CircuitBreaker>& breakers,
                                     std::string client_key,
                                     int max_attempts) {
    std::unordered_set<std::size_t> excluded;
    int attempts = 0;

    while (attempts < max_attempts) {
        auto backend_index = ring.get_backend_excluding(client_key, excluded);
        if (!backend_index) {
            // Every backend is either excluded (already tried) or the
            // ring is empty. Nothing left to try.
            break;
        }

        CircuitBreaker& breaker = breakers.at(*backend_index);
        if (!breaker.allow_request()) {
            // OPEN, or a HALF_OPEN trial is already in flight elsewhere -
            // skip this node without spending a network call on it.
            excluded.insert(*backend_index);
            ++attempts;
            continue;
        }

        std::optional<asio::ip::tcp::socket> backend_socket;
        try {
            backend_socket.emplace(co_await pool.acquire(*backend_index));
        } catch (const std::exception& e) {
            // Couldn't even establish a connection - no bytes have moved
            // in either direction, so this is unconditionally safe to
            // retry against a different backend.
            std::cerr << "[session] connect failed for backend " << *backend_index
                      << ": " << e.what() << "\n";
            breaker.record_failure();
            excluded.insert(*backend_index);
            ++attempts;
            continue;
        }

        std::atomic<bool> response_started{false};

        co_await (relay(client_socket, *backend_socket, nullptr) ||
                  relay(*backend_socket, client_socket, &response_started));

        // At raw L4 we can't see HTTP status codes, so "the backend sent
        // back at least one byte" is the only success signal available
        // here. A backend that accepts the connection, forwards a 500
        // body, and closes cleanly reads as a "success" to this breaker -
        // genuine 5xx-aware breaking needs response framing, which is
        // out of scope for a byte relay. Worth knowing as a real
        // limitation, not a bug: this breaker protects against dead/
        // unreachable nodes, not against nodes returning error bodies.
        bool got_any_response = response_started.load(std::memory_order_relaxed);

        // Same reasoning as Phase 2: we can't prove a used socket is
        // safe to reuse without response framing, so never recycle here.
        pool.release(*backend_index, std::move(*backend_socket), /*healthy=*/false);

        if (got_any_response) {
            breaker.record_success();
            co_return; // client already has data - this connection is done, win or lose
        }

        // No bytes ever reached the client - safe to retry elsewhere.
        breaker.record_failure();
        excluded.insert(*backend_index);
        ++attempts;
    }

    co_await send_status(client_socket, 503, "Service Unavailable",
                          "All backends unavailable\n");
}

} // namespace aegis