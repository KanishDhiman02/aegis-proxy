#include "rate_limiter.hpp"
#include <algorithm>

namespace aegis {

RateLimiter::RateLimiter(double refill_rate_per_sec, double capacity, std::size_t shard_count)
    : refill_rate_per_sec_(refill_rate_per_sec), capacity_(capacity), shards_(shard_count) {}

uint64_t RateLimiter::hash_key(const std::string& key) {
    // Same FNV-1a + avalanche finalizer used in HashRing (Phase 3). We
    // measured there that FNV-1a alone diffuses poorly on short, similar
    // inputs - the exact shape of IPs from one subnet or NAT gateway
    // (192.168.1.1, 192.168.1.2, ...). Without the finalizer, a whole
    // office or botnet subnet could land on the same shard, defeating
    // the purpose of sharding for precisely the clients most likely to
    // arrive concurrently.
    constexpr uint64_t fnv_offset_basis = 14695981039346656037ULL;
    constexpr uint64_t fnv_prime = 1099511628211ULL;
    uint64_t hash = fnv_offset_basis;
    for (unsigned char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= fnv_prime;
    }
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

std::size_t RateLimiter::shard_index(const std::string& client_ip) const {
    return hash_key(client_ip) % shards_.size();
}

bool RateLimiter::allow(const std::string& client_ip) {
    Shard& shard = shards_[shard_index(client_ip)];
    std::lock_guard<std::mutex> lock(shard.mutex);

    auto now = std::chrono::steady_clock::now();
    auto [it, inserted] = shard.buckets.try_emplace(client_ip, Bucket{capacity_, now});
    Bucket& bucket = it->second;

    if (!inserted) {
        double elapsed_sec = std::chrono::duration<double>(now - bucket.last_refill).count();
        bucket.tokens = std::min(capacity_, bucket.tokens + elapsed_sec * refill_rate_per_sec_);
        bucket.last_refill = now;
    }

    if (bucket.tokens >= 1.0) {
        bucket.tokens -= 1.0;
        return true;
    }
    return false;
}

std::size_t RateLimiter::reap_idle(std::chrono::steady_clock::duration idle_threshold) {
    auto now = std::chrono::steady_clock::now();
    std::size_t removed = 0;
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto it = shard.buckets.begin(); it != shard.buckets.end();) {
            if (now - it->second.last_refill > idle_threshold) {
                it = shard.buckets.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
    }
    return removed;
}

std::size_t RateLimiter::total_tracked_clients() const {
    std::size_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        total += shard.buckets.size();
    }
    return total;
}

} // namespace aegis
