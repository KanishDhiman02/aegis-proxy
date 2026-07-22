#include "hash_ring.hpp"
#include <cmath>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

using aegis::HashRing;

int main() {
    constexpr std::size_t kNumKeys = 100000;
    constexpr std::size_t kNumBackends = 5;

    std::vector<std::string> keys;
    keys.reserve(kNumKeys);
    for (std::size_t i = 0; i < kNumKeys; ++i) {
        // Simulates distinct client IPs / session ids.
        keys.push_back("client-" + std::to_string(i));
    }

    // --- Test 1: distribution evenness across 5 backends ---
    HashRing ring; // uses the default (500) vnodes per backend
    for (std::size_t b = 0; b < kNumBackends; ++b) {
        ring.add_backend(b);
    }

    std::map<std::size_t, std::size_t> counts;
    std::map<std::string, std::size_t> assignment_before;
    for (const auto& key : keys) {
        auto b = ring.get_backend(key);
        counts[*b]++;
        assignment_before[key] = *b;
    }

    double expected = static_cast<double>(kNumKeys) / kNumBackends;
    double max_deviation_pct = 0.0;
    std::printf("=== Distribution across %zu backends (default vnodes each, %zu keys) ===\n",
                kNumBackends, kNumKeys);
    for (const auto& [backend, count] : counts) {
        double deviation_pct = (static_cast<double>(count) - expected) / expected * 100.0;
        max_deviation_pct = std::max(max_deviation_pct, std::abs(deviation_pct));
        std::printf("  backend %zu: %6zu keys (%.2f%% deviation from even split of %.0f)\n",
                    backend, count, deviation_pct, expected);
    }
    std::printf("  max deviation from perfectly even split: %.2f%%\n\n", max_deviation_pct);

    // --- Test 2: reshuffle impact when one backend is removed ---
    ring.remove_backend(2); // simulate backend 2 failing

    std::size_t reassigned = 0;
    std::size_t reassigned_away_from_removed = 0;
    for (const auto& key : keys) {
        auto b = ring.get_backend(key);
        if (*b != assignment_before[key]) {
            reassigned++;
            if (assignment_before[key] == 2) {
                reassigned_away_from_removed++;
            }
        }
    }

    double reshuffle_pct = static_cast<double>(reassigned) / kNumKeys * 100.0;
    double expected_reshuffle_pct = 100.0 / kNumBackends; // ~1/N should move

    std::printf("=== Reshuffle impact: removing 1 of %zu backends ===\n", kNumBackends);
    std::printf("  total keys reassigned: %zu / %zu (%.2f%%)\n", reassigned, kNumKeys, reshuffle_pct);
    std::printf("  of those, keys that were actually on the removed backend: %zu\n",
                reassigned_away_from_removed);
    std::printf("  naive hash%%N would reassign ~100%% of keys; expected ~%.1f%% here\n\n",
                expected_reshuffle_pct);

    // --- Test 3: get_backend_excluding walks past excluded backends ---
    std::unordered_set<std::size_t> excluded = {0, 1};
    std::size_t excluding_failures = 0;
    for (const auto& key : keys) {
        auto b = ring.get_backend_excluding(key, excluded);
        if (!b || excluded.count(*b)) {
            excluding_failures++;
        }
    }
    std::printf("=== get_backend_excluding correctness (excluding backends 0,1) ===\n");
    std::printf("  keys incorrectly routed to an excluded backend: %zu / %zu\n\n",
                excluding_failures, kNumKeys);

    bool ok = max_deviation_pct < 8.0 && reassigned_away_from_removed == counts[2]
              && reshuffle_pct < 30.0 && excluding_failures == 0;
    std::printf("%s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
