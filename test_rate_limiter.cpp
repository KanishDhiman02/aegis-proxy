#include "rate_limiter.hpp"
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using aegis::RateLimiter;
using Clock = std::chrono::steady_clock;

bool test_burst_and_refill() {
    // capacity 5, refill 2 tokens/sec
    RateLimiter limiter(/*refill_rate_per_sec=*/2.0, /*capacity=*/5.0);

    int allowed = 0;
    for (int i = 0; i < 8; ++i) {
        if (limiter.allow("1.2.3.4")) ++allowed;
    }
    std::printf("burst test: %d/8 allowed immediately (expect 5)\n", allowed);
    if (allowed != 5) return false;

    std::this_thread::sleep_for(std::chrono::milliseconds(600)); // ~1.2 tokens back
    bool after_wait = limiter.allow("1.2.3.4");
    bool immediately_after = limiter.allow("1.2.3.4");
    std::printf("after 600ms wait: 1st=%d 2nd=%d (expect true,false - only ~1.2 tokens accumulated)\n",
                after_wait, immediately_after);

    return after_wait && !immediately_after;
}

bool test_independent_clients() {
    // Different client IPs must not share a bucket.
    RateLimiter limiter(1.0, 3.0);
    for (int i = 0; i < 3; ++i) {
        if (!limiter.allow("10.0.0.1")) return false;
    }
    // client A's bucket is now empty - client B should be unaffected
    bool b_allowed = limiter.allow("10.0.0.2");
    bool a_blocked = !limiter.allow("10.0.0.1");
    std::printf("independent clients: B allowed=%d, A (exhausted) blocked=%d\n", b_allowed, a_blocked);
    return b_allowed && a_blocked;
}

std::size_t test_reap() {
    RateLimiter limiter(1.0, 3.0);
    for (int i = 0; i < 100; ++i) {
        limiter.allow("client-" + std::to_string(i));
    }
    std::size_t before = limiter.total_tracked_clients();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::size_t removed = limiter.reap_idle(std::chrono::milliseconds(10));
    std::size_t after = limiter.total_tracked_clients();
    std::printf("reap test: tracked before=%zu, removed=%zu, tracked after=%zu (expect 100, 100, 0)\n",
                before, removed, after);
    return (before == 100 && removed == 100 && after == 0) ? 0 : 1;
}

// Measures wall-clock time for many threads, each repeatedly calling
// allow() on its OWN distinct client IP - this is the scenario lock
// sharding targets: many different concurrent clients, not one client
// hammering the limiter. Runs the same workload against a 1-shard
// limiter (global lock) and a 64-shard limiter to show the actual
// contention difference, rather than assert it.
double benchmark(std::size_t shard_count, unsigned num_threads, int ops_per_thread) {
    RateLimiter limiter(1e9, 1e9, shard_count); // effectively unlimited tokens - measuring lock overhead, not throttling
    std::vector<std::thread> threads;
    auto start = Clock::now();

    for (unsigned t = 0; t < num_threads; ++t) {
        threads.emplace_back([&limiter, t, ops_per_thread]() {
            std::string ip = "10.0." + std::to_string(t / 256) + "." + std::to_string(t % 256);
            for (int i = 0; i < ops_per_thread; ++i) {
                limiter.allow(ip);
            }
        });
    }
    for (auto& th : threads) th.join();

    auto elapsed = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    return elapsed;
}

int main() {
    bool ok = true;
    ok &= test_burst_and_refill();
    ok &= test_independent_clients();
    ok &= (test_reap() == 0);

    unsigned hw = std::thread::hardware_concurrency();
    unsigned num_threads = hw > 0 ? hw * 4 : 16; // oversubscribe to force real contention
    int ops_per_thread = 200000;

    std::printf("\n=== Contention benchmark: %u threads x %d ops each, distinct client IPs ===\n",
                num_threads, ops_per_thread);
    std::printf("hardware_concurrency reported: %u\n", hw);

    double t1 = benchmark(1, num_threads, ops_per_thread);
    double t64 = benchmark(64, num_threads, ops_per_thread);

    std::printf("1 shard  (global lock): %.1f ms\n", t1);
    std::printf("64 shards             : %.1f ms\n", t64);
    std::printf("speedup: %.2fx\n", t1 / t64);

    std::printf("\n%s\n", ok ? "CORRECTNESS: PASS" : "CORRECTNESS: FAIL");
    return ok ? 0 : 1;
}
