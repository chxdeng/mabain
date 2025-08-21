// Internal search engine to centralize search orchestration.
#pragma once

#include "detail/lf_guard.h"
#include "dict.h"
#include "util/prefix_cache_iface.h"
#include <cstdint>
#include <string>
#include <time.h>

namespace mabain {
class Dict;
struct MBData;

// Tracks the best "<= target" candidate while descending for lowerBound.
// - target key: original search key pointer
// - node buffer: scratch buffer for reading node/edge labels
// - bound_key: optional output accumulator for reconstructed lower-bound key
// - le_match_len/le_edge_key: depth and edge key for the best candidate seen
// - use_curr_edge: signal to pivot into current subtree when label < suffix
struct BoundSearchState {
    const uint8_t* key;
    uint8_t* node_buff;
    std::string* bound_key;

    int le_match_len = 0;
    int le_edge_key = -1;
    bool use_curr_edge = false;
};

namespace detail {

    namespace profile {
        bool enabled();
        void add_cache_probe(uint64_t ns);
        void add_root_step(uint64_t ns);
        void add_traverse(uint64_t ns);
        void add_resolve(uint64_t ns);
        void add_call();
        void reset();
        void snapshot(uint64_t& calls, uint64_t& ns_cache, uint64_t& ns_root,
            uint64_t& ns_traverse, uint64_t& ns_resolve);
    }

    class SearchEngine {
    public:
        explicit SearchEngine(Dict& d)
            : dict(d)
        {
        }

        // Exact match
        int find(const uint8_t* key, int len, MBData& data);

        // Longest prefix match
        int findPrefix(const uint8_t* key, int len, MBData& data);

        // Lower bound (largest entry not greater than key)
        int lowerBound(const uint8_t* key, int len, MBData& data, std::string* bound_key);

    private:
        Dict& dict;

        // Exact-find internals
        inline int tryFindAtRoot(size_t root_off, const uint8_t* key, int len, MBData& data);
        int findInternal(size_t root_off, const uint8_t* key, int len, MBData& data);
        int traverseFromEdge(const uint8_t*& key_cursor, int& len, int& consumed,
            const uint8_t* full_key, int full_len, EdgePtrs& edge_ptrs, MBData& data);

        // Prefix internals
        int findPrefixInternal(size_t root_off, const uint8_t* key, int len, MBData& data);
        int traversePrefixFromEdge(const uint8_t* key_base, const uint8_t*& key_cursor, int& len,
            EdgePtrs& edge_ptrs, MBData& data, int& last_prefix_rval, uint8_t last_node_buffer[NODE_EDGE_KEY_FIRST]) const;

        // Small helpers
        // Exact-match path helper: always copy edge tail into node_buff
        int loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const;
        inline bool remainderMatches(const uint8_t* key_buff, const uint8_t* p, int rem_len) const;
        inline bool isLeaf(const EdgePtrs& edge_ptrs) const;
        inline int compareCurrEdgeTail(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t* p,
            const uint8_t*& key_buff, int& edge_len, int& edge_len_m1) const;
        inline int resolveMatchOrInDict(MBData& data, EdgePtrs& edge_ptrs, bool at_root) const;
        // Root-edge accessor (reads directly from DictMem)
        // Fast-path: try to seed traversal state from the prefix cache.
        // Returns true when an entry is found (depth 2 or 3), and advances
        // key_cursor/len_remaining/consumed accordingly. For very short keys
        // (len < 2) it returns false without making a virtual call.
        inline bool seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
            MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const;
        // declared once above

        // Lower-bound internals
        void appendEdgeKey(std::string* key, int edge_key, const EdgePtrs& edge_ptrs) const;
        int readLowerBound(EdgePtrs& edge_ptrs, MBData& data, std::string* bound_key, int le_edge_key) const;
        int readBoundFromRootEdge(EdgePtrs& edge_ptrs, MBData& data, int root_key, std::string* bound_key) const;
        // Core lower-bound traversal from root-key edge until first resolution point.
        // Returns SUCCESS when value is read, NOT_EXIST when caller should pivot to
        // saved candidate (bound_edge_ptrs/use_curr_edge), or READ_ERROR on IO error.
        int lowerBoundCore(const uint8_t* key, int len, MBData& data, std::string* bound_key,
            EdgePtrs& bound_edge_ptrs, BoundSearchState& bound_state, int root_key) const;
        int traverseToLowerBound(const uint8_t* key, int len, EdgePtrs& edge_ptrs, MBData& data,
            EdgePtrs& bound_edge_ptrs, BoundSearchState& state) const;
    };

} // namespace detail
} // namespace mabain

// Inline implementations for hot exact-match path
namespace mabain {
namespace detail {

    inline int SearchEngine::tryFindAtRoot(size_t root_off, const uint8_t* key, int len, MBData& data)
    {
        int r = findInternal(root_off, key, len, data);
#ifdef __LOCK_FREE__
        int attempts = 0;
        while (r == MBError::TRY_AGAIN && attempts++ < CONSTS::LOCK_FREE_RETRY_LIMIT) {
            nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
            r = findInternal(root_off, key, len, data);
        }
#endif
        return r;
    }

    // moved out-of-line to search_engine.cpp for code size/perf tradeoffs

}
} // namespace mabain::detail

// Additional small inline helpers to avoid extra call overhead
namespace mabain {
namespace detail {

    inline bool SearchEngine::remainderMatches(const uint8_t* key_buff, const uint8_t* p, int rem_len) const
    {
        return (rem_len <= 0) || (memcmp(key_buff, p + 1, rem_len) == 0);
    }

    inline bool SearchEngine::isLeaf(const EdgePtrs& edge_ptrs) const
    {
        return (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) != 0;
    }

    inline int SearchEngine::compareCurrEdgeTail(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t* p,
        const uint8_t*& key_buff, int& edge_len, int& edge_len_m1) const
    {
        if (edge_len <= 0)
            return MBError::NOT_EXIST;
        int ret = loadEdgeKey(edge_ptrs, data, key_buff, edge_len_m1);
        if (ret != MBError::SUCCESS)
            return ret;
        return remainderMatches(key_buff, p, edge_len_m1) ? MBError::SUCCESS : MBError::NOT_EXIST;
    }

    inline int SearchEngine::resolveMatchOrInDict(MBData& data, EdgePtrs& edge_ptrs, bool at_root) const
    {
        if (data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT) {
            if (at_root) {
                data.edge_ptrs.curr_node_offset = dict.mm.GetRootOffset();
                data.edge_ptrs.curr_nt = 1;
                data.edge_ptrs.curr_edge_index = 0;
                data.edge_ptrs.parent_offset = data.edge_ptrs.offset;
            }
            return MBError::IN_DICT;
        }
        if (data.options & CONSTS::OPTION_KEY_ONLY) {
            return MBError::SUCCESS;
        }
        return dict.ReadDataFromEdge(data, edge_ptrs);
    }

    // Tiny per-thread cache of root edges to avoid a ReadData on hot prefixes.
    // Safe only when no writers are active; guarded by header->num_writer == 0.
    // (getRootEdgeFast removed; callers read via dict.mm.GetRootEdge)

    inline int SearchEngine::loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const
    {
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
            // Fast-path: get direct pointer into mmap region to avoid copy
            const uint8_t* p = dict.mm.GetShmPtr(edge_str_off, edge_len_m1);
            if (p == nullptr)
                return MBError::READ_ERROR;
            key_buff = p;
        } else {
            key_buff = edge_ptrs.ptr;
        }
        return MBError::SUCCESS;
    }

    inline bool SearchEngine::seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
        MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const
    {
        // Keys shorter than 2 bytes cannot hit the cache; avoid virtual call.
        if (len < 2 || key == nullptr)
            return false;
        PrefixCacheIface* pc = dict.ActivePrefixCache();
        if (!pc)
            return false;

        PrefixCacheEntry entry;
        int n;
        if (profile::enabled()) {
            struct timespec ts1, ts2;
            clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);
            n = pc->GetDepth(key, len, entry);
            clock_gettime(CLOCK_MONOTONIC_RAW, &ts2);
            uint64_t ns = (uint64_t)(ts2.tv_sec - ts1.tv_sec) * 1000000000ULL + (uint64_t)(ts2.tv_nsec - ts1.tv_nsec);
            profile::add_cache_probe(ns);
        } else {
            n = pc->GetDepth(key, len, entry);
        }
        if (n == 0)
            return false;

        // Use cached edge directly (shared and non-shared behave identically here).

        edge_ptrs.offset = entry.edge_offset;
        memcpy(edge_ptrs.edge_buff, entry.edge_buff, EDGE_SIZE);
        InitTempEdgePtrs(edge_ptrs);
        // Do not set READ_SAVED_EDGE here: that flag is reserved for
        // lock-free saved-edge handoff. Shared-cache seeding already
        // supplies a stable edge entry via edge_ptrs and does its own
        // writer-in-progress validation, so enabling the flag here can
        // cause unintended interaction with the lock-free path.

        key_cursor += n;
        len_remaining -= n;
        consumed += n;
        return true;
    }

}
} // namespace mabain::detail
