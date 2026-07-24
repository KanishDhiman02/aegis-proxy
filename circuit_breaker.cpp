#include "circuit_breaker.hpp"

namespace aegis {

CircuitBreaker::CircuitBreaker(int failure_threshold, int success_threshold,
                                std::chrono::milliseconds cooldown_period)
    : failure_threshold_(failure_threshold),
      success_threshold_(success_threshold),
      cooldown_period_(cooldown_period) {}

bool CircuitBreaker::allow_request() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == BreakerState::CLOSED) {
        return true;
    }

    if (state_ == BreakerState::OPEN) {
        auto now = std::chrono::steady_clock::now();
        if (now - opened_at_ < cooldown_period_) {
            return false; // still cooling down, fail fast
        }
        // Cooldown elapsed: open exactly one trial slot.
        state_ = BreakerState::HALF_OPEN;
        consecutive_successes_ = 0;
        half_open_trial_in_flight_ = true;
        return true;
    }

    // HALF_OPEN: only one trial request is allowed in flight at a time,
    // so a burst of concurrent requests doesn't hammer a node that has
    // only just started to recover.
    if (half_open_trial_in_flight_) {
        return false;
    }
    half_open_trial_in_flight_ = true;
    return true;
}

void CircuitBreaker::record_success() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == BreakerState::HALF_OPEN) {
        half_open_trial_in_flight_ = false;
        ++consecutive_successes_;
        if (consecutive_successes_ >= success_threshold_) {
            state_ = BreakerState::CLOSED;
            consecutive_failures_ = 0;
            consecutive_successes_ = 0;
        }
        return;
    }

    // CLOSED: a success resets the failure streak, since the threshold
    // is defined in terms of *consecutive* failures.
    consecutive_failures_ = 0;
}

void CircuitBreaker::record_failure() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (state_ == BreakerState::HALF_OPEN) {
        // The trial failed - not recovered yet. Back to OPEN immediately,
        // restart the cooldown, don't wait for a fresh failure streak.
        half_open_trial_in_flight_ = false;
        state_ = BreakerState::OPEN;
        opened_at_ = std::chrono::steady_clock::now();
        consecutive_successes_ = 0;
        return;
    }

    if (state_ == BreakerState::CLOSED) {
        ++consecutive_failures_;
        if (consecutive_failures_ >= failure_threshold_) {
            state_ = BreakerState::OPEN;
            opened_at_ = std::chrono::steady_clock::now();
        }
        return;
    }

    // OPEN: allow_request() should have prevented any real attempt from
    // reaching here. If it happens anyway (e.g. a stray call), don't
    // reset the cooldown timer - that would let a flood of failures
    // perpetually postpone the next recovery attempt.
}

BreakerState CircuitBreaker::state() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

} // namespace aegis
