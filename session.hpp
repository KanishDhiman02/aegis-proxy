#pragma once

#include <asio.hpp>
#include <atomic>
#include <deque>
#include <string>

#include "circuit_breaker.hpp"
#include "connection_pool.hpp"
#include "hash_ring.hpp"

namespace aegis {

// Handles one client connection end-to-end, including retry-on-failure
// across backends:
//
//   1. Ask the ring for a backend (excluding any already tried/tripped).
//   2. Check that backend's circuit breaker.
//   3. Acquire a pooled connection and relay bytes bidirectionally.
//   4. If the backend never sent back a single byte before failing,
//      it's safe to retry a different backend - no data reached the
//      client yet. If it DID send at least one byte before failing, the
//      client already has partial data in hand; retrying would mean
//      writing a second, different response into that same connection,
//      which corrupts the client's view regardless of the client's
//      language or framework - so we stop and simply end the connection.
//   5. Give up with HTTP 503 only if every attempt failed before any
//      response bytes were sent - so 503 is always a clean, whole
//      response, never mixed into a partially-streamed one.
asio::awaitable<void> handle_client(asio::ip::tcp::socket client_socket,
                                     ConnectionPool& pool,
                                     HashRing& ring,
                                     std::deque<CircuitBreaker>& breakers,
                                     std::string client_key,
                                     int max_attempts = 3);

} // namespace aegis