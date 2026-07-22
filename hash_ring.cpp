#include "hash_ring.hpp"
#include <mutex>

namespace aegis {

HashRing::HashRing(std::size_t num_vnodes_per_backend)
    : num_vnodes_per_backend_(num_vnodes_per_backend) {}

uint64_t HashRing::hash_key(const std::string& key) {
    // FNV-1a, 64-bit. Chosen over std::hash<std::string> deliberately:
    // libstdc++/libc++ don't guarantee std::hash is stable across
    // process runs (some implementations seed it per-process for
    // DoS-hardening), which would silently reshuffle the entire ring on
    // every proxy restart.
    constexpr uint64_t fnv_offset_basis = 14695981039346656037ULL;
    constexpr uint64_t fnv_prime = 1099511628211ULL;
    uint64_t hash = fnv_offset_basis;
    for (unsigned char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= fnv_prime;
    }

    // FNV-1a alone diffuses poorly on short, structurally similar inputs
    // like "0#0", "0#1", "0#2", ... or "client-1", "client-2", ... -
    // measured empirically: with only the FNV-1a output, 150 vnodes per
    // backend produced up to 61% deviation from an even split across 5
    // backends, and even 5000 vnodes per backend still left 15%
    // deviation. Running the output through this finalizer (the
    // murmur3-style avalanche mix used by SplitMix64) fixes that: every
    // input bit ends up affecting every output bit, so keys that differ
    // by one character no longer land near each other on the ring.
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

std::string HashRing::vnode_key(std::size_t backend_index, std::size_t vnode_index) {
    return std::to_string(backend_index) + "#" + std::to_string(vnode_index);
}

void HashRing::add_backend(std::size_t backend_index) {
    std::unique_lock lock(mutex_);
    if (active_backends_.count(backend_index)) {
        return; // already present, no-op
    }
    for (std::size_t v = 0; v < num_vnodes_per_backend_; ++v) {
        uint64_t h = hash_key(vnode_key(backend_index, v));
        ring_[h] = backend_index;
    }
    active_backends_.insert(backend_index);
}

void HashRing::remove_backend(std::size_t backend_index) {
    std::unique_lock lock(mutex_);
    if (!active_backends_.count(backend_index)) {
        return;
    }
    for (std::size_t v = 0; v < num_vnodes_per_backend_; ++v) {
        uint64_t h = hash_key(vnode_key(backend_index, v));
        ring_.erase(h);
    }
    active_backends_.erase(backend_index);
}

std::optional<std::size_t> HashRing::get_backend(const std::string& key) const {
    std::shared_lock lock(mutex_);
    if (ring_.empty()) {
        return std::nullopt;
    }
    uint64_t h = hash_key(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        it = ring_.begin(); // wrap around the ring
    }
    return it->second;
}

std::optional<std::size_t> HashRing::get_backend_excluding(
    const std::string& key, const std::unordered_set<std::size_t>& excluded) const {
    std::shared_lock lock(mutex_);
    if (ring_.empty()) {
        return std::nullopt;
    }

    uint64_t h = hash_key(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    auto start = it;
    do {
        if (!excluded.count(it->second)) {
            return it->second;
        }
        ++it;
        if (it == ring_.end()) {
            it = ring_.begin();
        }
    } while (it != start);

    return std::nullopt; // every registered backend is excluded
}

std::size_t HashRing::backend_count() const {
    std::shared_lock lock(mutex_);
    return active_backends_.size();
}

std::size_t HashRing::vnode_count() const {
    std::shared_lock lock(mutex_);
    return ring_.size();
}

} // namespace aegis
