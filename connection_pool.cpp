#include "connection_pool.hpp"
#include <iostream>

namespace aegis {

ConnectionPool::ConnectionPool(asio::io_context& io_context, std::vector<BackendConfig> backends)
    : io_context_(io_context),
      backends_(std::move(backends)),
      free_sockets_(backends_.size()),
      mutexes_(backends_.size()) {}

asio::awaitable<asio::ip::tcp::socket> ConnectionPool::connect(std::size_t backend_index) {
    const auto& cfg = backends_.at(backend_index);

    asio::ip::tcp::resolver resolver(io_context_);
    auto endpoints = co_await resolver.async_resolve(cfg.host, cfg.port, asio::use_awaitable);

    asio::ip::tcp::socket socket(io_context_);
    co_await asio::async_connect(socket, endpoints, asio::use_awaitable);

    // We forward small frames frequently and care about latency over
    // minimizing packet count, so disable Nagle's algorithm.
    socket.set_option(asio::ip::tcp::no_delay(true));

    co_return socket;
}

asio::awaitable<void> ConnectionPool::warm_up() {
    for (std::size_t i = 0; i < backends_.size(); ++i) {
        const auto& cfg = backends_[i];
        std::size_t connected = 0;
        for (std::size_t n = 0; n < cfg.min_pool_size; ++n) {
            try {
                auto socket = co_await connect(i);
                std::lock_guard<std::mutex> lock(mutexes_[i]);
                free_sockets_[i].push_back(std::move(socket));
                ++connected;
            } catch (const std::exception& e) {
                std::cerr << "[pool] warm-up connect failed for " << cfg.host << ":" << cfg.port
                          << " -> " << e.what() << "\n";
            }
        }
        std::cout << "[pool] backend " << cfg.host << ":" << cfg.port
                  << " warmed with " << connected << "/" << cfg.min_pool_size << " sockets\n";
    }
}

asio::awaitable<asio::ip::tcp::socket> ConnectionPool::acquire(std::size_t backend_index) {
    {
        std::lock_guard<std::mutex> lock(mutexes_.at(backend_index));
        auto& free_list = free_sockets_.at(backend_index);
        if (!free_list.empty()) {
            asio::ip::tcp::socket socket = std::move(free_list.back());
            free_list.pop_back();
            co_return socket;
        }
    }
    // Pool exhausted: open a fresh connection rather than block the request.
    auto socket = co_await connect(backend_index);
    co_return socket;
}

void ConnectionPool::release(std::size_t backend_index, asio::ip::tcp::socket socket, bool healthy) {
    if (!healthy || !socket.is_open()) {
        return; // let it destruct/close - don't recycle a broken socket
    }
    std::lock_guard<std::mutex> lock(mutexes_.at(backend_index));
    free_sockets_.at(backend_index).push_back(std::move(socket));
}

} // namespace aegis
