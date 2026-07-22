#pragma once

#include <asio.hpp>
#include "connection_pool.hpp"

namespace aegis {

// Handles one client connection end-to-end: borrows a backend socket
// from the pool, relays raw bytes in both directions concurrently, and
// returns the backend socket to the pool when the connection ends.
//
// Phase 2 scope: raw byte relay to a fixed backend_index. Phase 3
// (routing) replaces the caller's fixed index with a hash-ring lookup -
// this function doesn't need to change when that happens.
asio::awaitable<void> handle_client(asio::ip::tcp::socket client_socket,
                                     ConnectionPool& pool,
                                     std::size_t backend_index);

} // namespace aegis
