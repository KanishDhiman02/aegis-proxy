#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_set>

namespace aegis {

// Consistent hash ring with virtual nodes (VNodes).
//
// Maps arbitrary string keys (client IP, session id, etc.) to a backend
// index, such that adding or removing one physical backend only
// reshuffles that backend's share of keys - roughly 1/N of them, not
// the whole ring, unlike a plain `hash(key) % N` scheme where every
// single key's target changes when N changes.
//
// Backed by std::map (red-black tree) over vnode hash -> backend index,
// giving O(log(N * vnodes_per_backend)) lookups.
//
// Thread-safety: reads (get_backend) happen on every accepted
// connection and are far more frequent than writes (add/remove_backend,
// which only happen when the health checker detects a state change), so
// this uses a shared_mutex - many concurrent readers, exclusive writer.
class HashRing {
public:
    // num_vnodes_per_backend: how many points on the ring each physical
    // backend occupies. Default of 500 is measured, not guessed - see
    // test_hash_ring.cpp: at 150 vnodes, 5 backends saw up to ~15%
    // deviation from an even split across 100k keys; at 500, that drops
    // to ~4%. Lookups stay O(log(N * vnodes)) either way, so the cost of
    // going from 150 to 500 is a few hundred extra std::map entries, not
    // a measurable latency difference.
    explicit HashRing(std::size_t num_vnodes_per_backend = 500);

    void add_backend(std::size_t backend_index);
    void remove_backend(std::size_t backend_index);

    // Returns the backend responsible for `key`, or std::nullopt if the
    // ring has no registered backends.
    std::optional<std::size_t> get_backend(const std::string& key) const;

    // Same lookup, but skips any backend already in `excluded` - used by
    // the retry loop so a request doesn't get routed back to a node
    // whose circuit breaker is already open.
    std::optional<std::size_t> get_backend_excluding(
        const std::string& key, const std::unordered_set<std::size_t>& excluded) const;

    std::size_t backend_count() const;
    std::size_t vnode_count() const;

private:
    static uint64_t hash_key(const std::string& key);
    static std::string vnode_key(std::size_t backend_index, std::size_t vnode_index);

    std::size_t num_vnodes_per_backend_;
    std::map<uint64_t, std::size_t> ring_; // vnode hash -> backend_index
    std::unordered_set<std::size_t> active_backends_;
    mutable std::shared_mutex mutex_;
};

} // namespace aegis
