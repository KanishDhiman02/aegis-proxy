#pragma once

#include <asio.hpp>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace aegis {

struct BackendConfig {
    std::string host;
    std::string port;
    std::size_t min_pool_size = 4;
};

// Manages a set of pre-warmed, keep-alive TCP sockets per backend so the
// proxy doesn't pay a fresh TCP handshake - and doesn't chew through
// ephemeral client ports - on every forwarded request.
//
// Thread-safety note: each backend's free list is protected by its own
// mutex (not one global lock, so contention on backend A's pool doesn't
// block backend B). The mutex is only ever held around the deque
// push/pop itself, never across a co_await, so it's safe to use even
// though the surrounding functions are coroutines.
class ConnectionPool {
public:
    ConnectionPool(asio::io_context& io_context, std::vector<BackendConfig> backends);

    // Pre-connects min_pool_size sockets for every backend. Call once at
    // startup, before accepting client traffic, so the first real
    // requests don't pay connection-setup latency.
    asio::awaitable<void> warm_up();

    // Hands out a socket for the given backend. If the pool is empty, a
    // fresh connection is opened on demand rather than blocking or
    // failing the request - repeated exhaustion is a signal to raise
    // min_pool_size, not something that should surface as a client error.
    asio::awaitable<asio::ip::tcp::socket> acquire(std::size_t backend_index);

    // Returns a socket to the pool for reuse. If healthy is false (the
    // backend reset the connection, or a relay error occurred), the
    // socket is dropped instead of recycled.
    void release(std::size_t backend_index, asio::ip::tcp::socket socket, bool healthy = true);

    std::size_t backend_count() const { return backends_.size(); }
    const BackendConfig& backend_config(std::size_t index) const { return backends_.at(index); }

private:
    asio::awaitable<asio::ip::tcp::socket> connect(std::size_t backend_index);

    asio::io_context& io_context_;
    std::vector<BackendConfig> backends_;
    std::vector<std::deque<asio::ip::tcp::socket>> free_sockets_;
    std::vector<std::mutex> mutexes_;
};

} // namespace aegis
