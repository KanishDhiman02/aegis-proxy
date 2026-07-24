#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace aegis {

// Sharded token-bucket rate limiter. Each client IP gets its own token
// bucket; buckets are partitioned across N independent shards (each
// with its own mutex and its own unordered_map) so that concurrent
// requests from different clients rarely contend for the same lock -
// only requests that happen to hash to the same shard do.
class RateLimiter {
public:
    // refill_rate_per_sec: tokens added per second per client.
    // capacity: max tokens a single client's bucket can hold (the
    // largest burst a client can send before throttling kicks in).
    // shard_count: number of independent lock+map partitions.
    RateLimiter(double refill_rate_per_sec, double capacity, std::size_t shard_count = 64);

    // Returns true if the request is allowed (a token was available and
    // consumed), false if the client's bucket is empty - caller should
    // respond with HTTP 429 in that case.
    bool allow(const std::string& client_ip);

    // Drops buckets untouched for longer than idle_threshold. Without
    // this, a map keyed on client IP grows without bound under IP churn
    // (NAT pools, rotating addresses, spoofed source IPs). Meant to be
    // called periodically from a background timer, not per-request.
    std::size_t reap_idle(std::chrono::steady_clock::duration idle_threshold);

    std::size_t shard_count() const { return shards_.size(); }
    std::size_t total_tracked_clients() const;

private:
    struct Bucket {
        double tokens;
        std::chrono::steady_clock::time_point last_refill;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, Bucket> buckets;
    };

    std::size_t shard_index(const std::string& client_ip) const;
    static uint64_t hash_key(const std::string& key);

    double refill_rate_per_sec_;
    double capacity_;
    std::vector<Shard> shards_;
};

} // namespace aegis
