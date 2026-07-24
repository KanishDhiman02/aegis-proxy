#pragma once

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace aegis {

// All counters are plain atomics, incremented with relaxed ordering -
// these are monotonic counters read for dashboards/scraping, not used
// to synchronize access to anything else, so relaxed is sufficient and
// cheaper than the default seq_cst under contention.
//
// Per-backend vectors are sized once at construction (see the same
// vector<atomic<T>>(count) pattern used in ConnectionPool/RateLimiter -
// std::atomic isn't movable or copyable, so this must be a direct sized
// construction, never reserve()+push_back()).
class MetricsRegistry {
public:
    explicit MetricsRegistry(std::size_t backend_count)
        : backend_success_(backend_count),
          backend_failure_(backend_count),
          breaker_state_(backend_count),
          pool_active_(backend_count),
          pool_idle_(backend_count) {}

    void record_request_received() { total_requests_.fetch_add(1, std::memory_order_relaxed); }
    void record_rate_limited() { rate_limited_.fetch_add(1, std::memory_order_relaxed); }
    void record_fallback_503() { fallback_503_.fetch_add(1, std::memory_order_relaxed); }

    void record_backend_success(std::size_t backend_index) {
        backend_success_.at(backend_index).fetch_add(1, std::memory_order_relaxed);
    }
    void record_backend_failure(std::size_t backend_index) {
        backend_failure_.at(backend_index).fetch_add(1, std::memory_order_relaxed);
    }

    // state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN - matches aegis::BreakerState's
    // underlying enum order, kept as a plain int here so metrics.hpp
    // doesn't need to depend on circuit_breaker.hpp.
    void set_breaker_state(std::size_t backend_index, int state) {
        breaker_state_.at(backend_index).store(state, std::memory_order_relaxed);
    }

    void set_pool_active(std::size_t backend_index, int count) {
        pool_active_.at(backend_index).store(count, std::memory_order_relaxed);
    }
    void set_pool_idle(std::size_t backend_index, int count) {
        pool_idle_.at(backend_index).store(count, std::memory_order_relaxed);
    }

    // Prometheus text exposition format (the de facto standard scrape
    // format) - so this can sit behind a real /metrics endpoint that any
    // standard scraper (Prometheus, Grafana Agent, Datadog's OpenMetrics
    // input) understands with zero custom parsing.
    std::string render_prometheus() const {
        std::ostringstream oss;

        oss << "# HELP aegis_requests_total Total requests accepted by the listener\n"
            << "# TYPE aegis_requests_total counter\n"
            << "aegis_requests_total " << total_requests_.load(std::memory_order_relaxed) << "\n";

        oss << "# HELP aegis_rate_limited_total Requests rejected with HTTP 429\n"
            << "# TYPE aegis_rate_limited_total counter\n"
            << "aegis_rate_limited_total " << rate_limited_.load(std::memory_order_relaxed) << "\n";

        oss << "# HELP aegis_fallback_503_total Requests exhausted all retry attempts\n"
            << "# TYPE aegis_fallback_503_total counter\n"
            << "aegis_fallback_503_total " << fallback_503_.load(std::memory_order_relaxed) << "\n";

        oss << "# HELP aegis_backend_success_total Successful relays per backend\n"
            << "# TYPE aegis_backend_success_total counter\n";
        for (std::size_t i = 0; i < backend_success_.size(); ++i) {
            oss << "aegis_backend_success_total{backend=\"" << i << "\"} "
                << backend_success_[i].load(std::memory_order_relaxed) << "\n";
        }

        oss << "# HELP aegis_backend_failure_total Failed attempts per backend\n"
            << "# TYPE aegis_backend_failure_total counter\n";
        for (std::size_t i = 0; i < backend_failure_.size(); ++i) {
            oss << "aegis_backend_failure_total{backend=\"" << i << "\"} "
                << backend_failure_[i].load(std::memory_order_relaxed) << "\n";
        }

        oss << "# HELP aegis_breaker_state Circuit breaker state per backend "
               "(0=CLOSED, 1=OPEN, 2=HALF_OPEN)\n"
            << "# TYPE aegis_breaker_state gauge\n";
        for (std::size_t i = 0; i < breaker_state_.size(); ++i) {
            oss << "aegis_breaker_state{backend=\"" << i << "\"} "
                << breaker_state_[i].load(std::memory_order_relaxed) << "\n";
        }

        oss << "# HELP aegis_pool_active_connections Checked-out connections per backend\n"
            << "# TYPE aegis_pool_active_connections gauge\n";
        for (std::size_t i = 0; i < pool_active_.size(); ++i) {
            oss << "aegis_pool_active_connections{backend=\"" << i << "\"} "
                << pool_active_[i].load(std::memory_order_relaxed) << "\n";
        }

        oss << "# HELP aegis_pool_idle_connections Idle (pooled, warm) connections per backend\n"
            << "# TYPE aegis_pool_idle_connections gauge\n";
        for (std::size_t i = 0; i < pool_idle_.size(); ++i) {
            oss << "aegis_pool_idle_connections{backend=\"" << i << "\"} "
                << pool_idle_[i].load(std::memory_order_relaxed) << "\n";
        }

        return oss.str();
    }

private:
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> rate_limited_{0};
    std::atomic<uint64_t> fallback_503_{0};
    std::vector<std::atomic<uint64_t>> backend_success_;
    std::vector<std::atomic<uint64_t>> backend_failure_;
    std::vector<std::atomic<int>> breaker_state_;
    std::vector<std::atomic<int>> pool_active_;
    std::vector<std::atomic<int>> pool_idle_;
};

} // namespace aegis
