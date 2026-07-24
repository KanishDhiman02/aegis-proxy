#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace aegis {

// Generates correlation IDs unique within this process's lifetime, and
// (via the epoch-ms prefix) very unlikely to collide across restarts
// either. Deliberately not a full UUID: no external dependency, and a
// monotonically increasing suffix makes IDs sortable by arrival order in
// logs, which a random UUID would not give us for free.
class CorrelationId {
public:
    static std::string generate() {
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
        uint64_t seq = counter_.fetch_add(1, std::memory_order_relaxed);
        std::ostringstream oss;
        oss << "req-" << now_ms << "-" << seq;
        return oss.str();
    }

private:
    static inline std::atomic<uint64_t> counter_{0};
};

// Structured, single-line, key=value logging - the format log
// aggregation tools (ELK, Datadog, Splunk) parse without needing a
// custom grammar. Every line carries the correlation ID first, so a
// `grep cid=req-172...` pulls every log line touched by one request:
// the rate-limit decision, the backend chosen, any retries, and the
// final outcome, in order.
class TraceLogger {
public:
    explicit TraceLogger(std::string correlation_id) : correlation_id_(std::move(correlation_id)) {}

    void event(const std::string& event_name,
               const std::vector<std::pair<std::string, std::string>>& fields = {}) const {
        std::ostringstream oss;
        oss << "cid=" << correlation_id_ << " event=" << event_name;
        for (const auto& [key, value] : fields) {
            oss << " " << key << "=" << value;
        }
        // Trace lines go to stderr, same stream as the rest of the
        // proxy's operational logs - stdout is left free for anything
        // that should stay clean (currently unused, but keeps the door
        // open for a future machine-readable stdout stream).
        std::cerr << oss.str() << "\n";
    }

    const std::string& correlation_id() const { return correlation_id_; }

private:
    std::string correlation_id_;
};

} // namespace aegis
