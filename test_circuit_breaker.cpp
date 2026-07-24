#include "circuit_breaker.hpp"
#include <cassert>
#include <cstdio>
#include <thread>

using aegis::BreakerState;
using aegis::CircuitBreaker;

bool test_trips_after_threshold_failures() {
    CircuitBreaker cb(/*failure_threshold=*/3, /*success_threshold=*/2,
                       std::chrono::milliseconds(200));
    bool ok = true;
    for (int i = 0; i < 3; ++i) {
        ok &= cb.allow_request();
        cb.record_failure();
    }
    std::printf("after 3 consecutive failures: state=%d (expect OPEN=1)\n",
                static_cast<int>(cb.state()));
    ok &= (cb.state() == BreakerState::OPEN);

    // While OPEN and within cooldown, must fail fast (no trial granted).
    ok &= !cb.allow_request();
    std::printf("during cooldown: allow_request=false as expected: %s\n", ok ? "yes" : "NO");
    return ok;
}

bool test_half_open_single_trial_gate() {
    CircuitBreaker cb(2, 2, std::chrono::milliseconds(100));
    cb.record_failure();
    cb.record_failure(); // trips OPEN

    std::this_thread::sleep_for(std::chrono::milliseconds(150)); // cooldown elapses

    bool first_trial = cb.allow_request();  // should transition to HALF_OPEN and grant the trial
    bool second_concurrent = cb.allow_request(); // trial already in flight - must be rejected
    std::printf("half-open gate: first_trial=%d second_concurrent(should be blocked)=%d\n",
                first_trial, second_concurrent);

    bool ok = first_trial && !second_concurrent;
    ok &= (cb.state() == BreakerState::HALF_OPEN);
    return ok;
}

bool test_half_open_failure_reopens_immediately() {
    CircuitBreaker cb(1, 2, std::chrono::milliseconds(50));
    cb.record_failure(); // trips OPEN (threshold 1)
    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    bool trial = cb.allow_request(); // HALF_OPEN trial granted
    cb.record_failure();             // trial fails
    std::printf("after failed half-open trial: trial_granted=%d state=%d (expect OPEN=1)\n",
                trial, static_cast<int>(cb.state()));

    bool ok = trial && (cb.state() == BreakerState::OPEN);
    ok &= !cb.allow_request(); // fresh cooldown, must not immediately allow another trial
    return ok;
}

bool test_half_open_success_threshold_closes() {
    CircuitBreaker cb(1, 2, std::chrono::milliseconds(50));
    cb.record_failure(); // OPEN
    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    bool trial1 = cb.allow_request();
    cb.record_success(); // 1st consecutive success in HALF_OPEN
    std::printf("after 1st half-open success: state=%d (expect HALF_OPEN=2)\n",
                static_cast<int>(cb.state()));
    bool still_half_open = (cb.state() == BreakerState::HALF_OPEN);

    bool trial2 = cb.allow_request(); // 2nd trial permitted (previous one completed)
    cb.record_success();              // 2nd consecutive success -> should close
    std::printf("after 2nd half-open success: state=%d (expect CLOSED=0)\n",
                static_cast<int>(cb.state()));
    bool closed = (cb.state() == BreakerState::CLOSED);

    return trial1 && trial2 && still_half_open && closed;
}

bool test_success_resets_failure_streak_in_closed() {
    CircuitBreaker cb(3, 2, std::chrono::milliseconds(100));
    cb.record_failure();
    cb.record_failure();
    cb.record_success(); // should reset the streak
    cb.record_failure();
    cb.record_failure();
    // 2 consecutive failures since the reset - not yet at threshold of 3
    bool still_closed = (cb.state() == BreakerState::CLOSED);
    std::printf("2 failures after a success reset: state=%d (expect CLOSED=0)\n",
                static_cast<int>(cb.state()));
    return still_closed;
}

int main() {
    bool ok = true;
    ok &= test_trips_after_threshold_failures();
    ok &= test_half_open_single_trial_gate();
    ok &= test_half_open_failure_reopens_immediately();
    ok &= test_half_open_success_threshold_closes();
    ok &= test_success_resets_failure_streak_in_closed();

    std::printf("\n%s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
