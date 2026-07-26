#include "session.hpp"
#include <asio/experimental/awaitable_operators.hpp>
#include <array>
#include <iostream>
#include <optional>
#include <unordered_set>

namespace aegis {

using namespace asio::experimental::awaitable_operators;

namespace {

// Accumulates the client's request bytes as they're read, up to a cap,
// so a failed attempt can be retried against a different backend by
// replaying what the client already sent - it will not arrive again on
// its own, since the client only sends it once. Capped rather than
// unbounded: a large streamed request body would otherwise force us to
// hold the whole thing in memory just in case of a late retry, which
// doesn't scale. Once capped, retries stop being attempted (see
// handle_client) rather than silently replaying a truncated request to
// a new backend.
struct RequestBuffer {
    static constexpr std::size_t kCap = 16 * 1024;
    std::string data;
    bool capped = false;

    void append(const char* p, std::size_t n) {
        if (capped) return;
        if (data.size() + n > kCap) {
            capped = true;
            return;
        }
        data.append(p, n);
    }
};

// Relays bytes from `from` to `to` until `from` is closed or errors. If
// `capture` is non-null, every chunk read from `from` is also appended
// there before being forwarded - used on the client->backend leg so a
// failed attempt's bytes can be replayed to the next backend. If
// `response_started` is non-null, it's set the moment the FIRST chunk
// is forwarded - this is the retry-safety signal: it records "the
// client has received at least one byte", independent of which relay
// direction "wins" the operator|| race below, since the flag is set as
// a side effect during execution, not from the return value.
asio::awaitable<void> relay(asio::ip::tcp::socket& from, asio::ip::tcp::socket& to,
                             std::atomic<bool>* response_started, RequestBuffer* capture) {
    std::array<char, 8192> buffer;
    asio::error_code ec;

    for (;;) {
        std::size_t n = co_await from.async_read_some(
            asio::buffer(buffer), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            co_return;
        }

        if (capture) {
            capture->append(buffer.data(), n);
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

// Builds TraceLogger field vectors via emplace_back rather than an
// initializer-list of temporary pairs. This isn't just style: a real
// SIGBUS crash (confirmed via lldb backtrace, memmove blowing past a
// mapped page while copy-constructing a pair<string,string>) traced
// back to exactly this pattern - `log.event("x", {{"k", v}, ...})` -
// used directly at coroutine call sites. The initializer_list's backing
// array is a temporary whose lifetime/ABI handling did not reliably
// survive a coroutine suspend/resume boundary (e.g. resuming after
// co_await pool.acquire()) on AppleClang/ARM64, even though it was
// never flagged by ASan/UBSan and never reproduced on Linux/GCC.
// Building the vector explicitly sidesteps the fragile temporary
// entirely, regardless of the exact ABI mechanism at fault.
std::vector<std::pair<std::string, std::string>> fields(
    std::string k1, std::string v1) {
    std::vector<std::pair<std::string, std::string>> f;
    f.emplace_back(std::move(k1), std::move(v1));
    return f;
}
std::vector<std::pair<std::string, std::string>> fields(
    std::string k1, std::string v1, std::string k2, std::string v2) {
    std::vector<std::pair<std::string, std::string>> f;
    f.emplace_back(std::move(k1), std::move(v1));
    f.emplace_back(std::move(k2), std::move(v2));
    return f;
}

} // namespace

asio::awaitable<void> handle_client(asio::ip::tcp::socket client_socket,
                                     ConnectionPool& pool,
                                     HashRing& ring,
                                     std::deque<CircuitBreaker>& breakers,
                                     std::string client_key,
                                     std::string correlation_id,
                                     MetricsRegistry& metrics,
                                     int max_attempts) {
    TraceLogger log(std::move(correlation_id));
    log.event("request_received", fields("client", client_key));
    metrics.record_request_received();

    std::unordered_set<std::size_t> excluded;
    RequestBuffer req_buf;
    int attempts = 0;

    while (attempts < max_attempts) {
        auto backend_index = ring.get_backend_excluding(client_key, excluded);
        if (!backend_index) {
            log.event("ring_exhausted", fields("attempts", std::to_string(attempts)));
            break;
        }
        std::string idx_str = std::to_string(*backend_index);

        CircuitBreaker& breaker = breakers.at(*backend_index);
        metrics.set_breaker_state(*backend_index, static_cast<int>(breaker.state()));

        if (!breaker.allow_request()) {
            log.event("breaker_skip", fields("backend", idx_str, "reason", "open_or_trial_busy"));
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
            log.event("connect_failed", fields("backend", idx_str, "error", e.what()));
            breaker.record_failure();
            metrics.record_backend_failure(*backend_index);
            metrics.set_breaker_state(*backend_index, static_cast<int>(breaker.state()));
            excluded.insert(*backend_index);
            ++attempts;
            continue;
        }

        log.event("backend_selected", fields("backend", idx_str, "attempt", std::to_string(attempts + 1)));
        metrics.set_pool_active(*backend_index, pool.active_count(*backend_index));
        metrics.set_pool_idle(*backend_index, pool.idle_count(*backend_index));

        // Replay whatever we've already captured from the client to this
        // (possibly new) backend before relaying any further live bytes -
        // this is what makes retry actually work once a prior attempt
        // has already consumed some of the client's request.
        if (!req_buf.data.empty()) {
            asio::error_code ec;
            co_await asio::async_write(*backend_socket, asio::buffer(req_buf.data),
                                        asio::redirect_error(asio::use_awaitable, ec));
            if (ec) {
                log.event("replay_failed", fields("backend", idx_str));
                breaker.record_failure();
                metrics.record_backend_failure(*backend_index);
                metrics.set_breaker_state(*backend_index, static_cast<int>(breaker.state()));
                pool.release(*backend_index, std::move(*backend_socket), /*healthy=*/false);
                excluded.insert(*backend_index);
                ++attempts;
                continue;
            }
        }

        std::atomic<bool> response_started{false};

        co_await (relay(client_socket, *backend_socket, nullptr, &req_buf) ||
                  relay(*backend_socket, client_socket, &response_started, nullptr));

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
        metrics.set_pool_active(*backend_index, pool.active_count(*backend_index));
        metrics.set_pool_idle(*backend_index, pool.idle_count(*backend_index));

        if (got_any_response) {
            breaker.record_success();
            metrics.record_backend_success(*backend_index);
            metrics.set_breaker_state(*backend_index, static_cast<int>(breaker.state()));
            log.event("request_succeeded", fields("backend", idx_str));
            co_return; // client already has data - this connection is done, win or lose
        }

        // No bytes ever reached the client - safe to retry elsewhere.
        breaker.record_failure();
        metrics.record_backend_failure(*backend_index);
        metrics.set_breaker_state(*backend_index, static_cast<int>(breaker.state()));
        log.event("backend_failed_no_response", fields("backend", idx_str));
        excluded.insert(*backend_index);
        ++attempts;

        if (req_buf.capped) {
            // Too much of the request has already been sent to a backend
            // that then failed - we can no longer prove a replay to a new
            // backend would be a faithful copy of what the client
            // intended. Stop retrying rather than forward a silently
            // truncated request.
            log.event("retry_abandoned_buffer_capped", fields("buffered_bytes", std::to_string(req_buf.data.size())));
            break;
        }
    }

    metrics.record_fallback_503();
    log.event("fallback_503", fields("attempts", std::to_string(attempts)));
    co_await send_status(client_socket, 503, "Service Unavailable",
                          "All backends unavailable\n");
}

} // namespace aegis