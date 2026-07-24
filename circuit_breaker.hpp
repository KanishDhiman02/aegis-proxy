#pragma once

#include <chrono>
#include <mutex>

namespace aegis {

enum class BreakerState { CLOSED, OPEN, HALF_OPEN };

// Per-backend circuit breaker. CLOSED lets everything through and counts
// consecutive failures; past failure_threshold it trips OPEN and fails
// every request immediately (no network call) until cooldown_period has
// elapsed, at which point it allows exactly ONE trial request through
// (HALF_OPEN) to test recovery - any other request arriving while that
// trial is in flight is failed fast too, not queued as a second trial.
// success_threshold consecutive trial successes closes it again; a
// single trial failure sends it straight back to OPEN.
//
// Thread-safe via a single mutex - state transitions are simple integer
///enum updates, so contention cost is negligible even under concurrent
// access from many sessions.
class CircuitBreaker {
public:
    CircuitBreaker(int failure_threshold, int success_threshold,
                   std::chrono::milliseconds cooldown_period);

    // Call before attempting a request against this backend. Returns
    // true if the request should proceed (CLOSED, or this is the one
    // permitted HALF_OPEN trial). Returns false if the breaker is OPEN
    // (cooldown not yet elapsed) or if a HALF_OPEN trial is already in
    // flight - the caller should skip this backend and try another.
    bool allow_request();

    // Call after a request that did make it through allow_request().
    void record_success();
    void record_failure();

    BreakerState state() const;

private:
    mutable std::mutex mutex_;
    BreakerState state_ = BreakerState::CLOSED;

    int failure_threshold_;
    int success_threshold_;
    std::chrono::milliseconds cooldown_period_;

    int consecutive_failures_ = 0;
    int consecutive_successes_ = 0;
    std::chrono::steady_clock::time_point opened_at_;
    bool half_open_trial_in_flight_ = false;
};

} // namespace aegis
