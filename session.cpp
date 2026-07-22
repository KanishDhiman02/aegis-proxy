#include "session.hpp"
#include <asio/experimental/awaitable_operators.hpp>
#include <array>
#include <iostream>

namespace aegis {

using namespace asio::experimental::awaitable_operators;

namespace {

// Relays bytes from `from` to `to` until `from` is closed or errors.
// Returns true if the stream ended cleanly (EOF), false on any other
// error - the caller uses this to decide whether the backend socket is
// still healthy enough to recycle into the pool.
asio::awaitable<bool> relay(asio::ip::tcp::socket& from, asio::ip::tcp::socket& to) {
    std::array<char, 8192> buffer;
    asio::error_code ec;

    for (;;) {
        std::size_t n = co_await from.async_read_some(
            asio::buffer(buffer), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            co_return ec == asio::error::eof;
        }

        co_await asio::async_write(
            to, asio::buffer(buffer, n), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            co_return false;
        }
    }
}

} // namespace

asio::awaitable<void> handle_client(asio::ip::tcp::socket client_socket,
                                     ConnectionPool& pool,
                                     std::size_t backend_index) {
    asio::ip::tcp::socket backend_socket = co_await pool.acquire(backend_index);
    bool healthy = true;

    try {
        // Race the two directions, not gather them: whichever side ends
        // first (client disconnects, or backend closes / errors) should
        // tear the whole session down immediately. operator|| cancels
        // the still-pending leg automatically. Using && here instead
        // would leave the other direction's read stuck forever whenever
        // the backend closes after one exchange (e.g. "Connection:
        // close"), silently hanging every subsequent request that reuses
        // that leaked, half-dead session.
        co_await (relay(client_socket, backend_socket) ||
                  relay(backend_socket, client_socket));

        // At raw L4 we cannot tell whether the backend just finished one
        // logical response on a connection it intends to keep alive, or
        // closed the connection outright - that distinction needs
        // HTTP-aware framing (Content-Length / chunked parsing), which is
        // out of scope for the raw byte relay. So for now we never
        // recycle a socket that has actually carried traffic; only
        // warm-up connections that were never used get reused as-is.
        // TODO(phase 3/4): once request/response framing exists, detect
        // "backend response complete, connection still open" and return
        // the socket to the pool instead of always discarding it here.
        healthy = false;
    } catch (const std::exception& e) {
        std::cerr << "[session] relay error: " << e.what() << "\n";
        healthy = false;
    }

    pool.release(backend_index, std::move(backend_socket), healthy);
}

} // namespace aegis
