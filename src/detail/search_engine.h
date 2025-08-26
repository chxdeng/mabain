// Internal search engine to centralize search orchestration.
#pragma once

#include "detail/lf_guard.h"
#include "dict.h"
#include "util/prefix_cache.h"
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

    // Removed obsolete profiling hooks

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
            EdgePtrs& edge_ptrs, MBData& data, int& last_prefix_rval,
            uint8_t last_node_buffer[NODE_EDGE_KEY_FIRST]) const;

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

    // Reads root edges via DictMem; no per-thread root cache.

    inline int SearchEngine::loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const
    {
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
            // Prefer direct pointer into mmap region to avoid a copy. If that
            // region is not currently mapped (e.g., small memcap or sliding
            // window disabled), fall back to a buffered read.
            const uint8_t* p = dict.mm.GetShmPtr(edge_str_off, edge_len_m1);
            if (p == nullptr) {
                if (dict.mm.ReadData(data.node_buff, edge_len_m1, edge_str_off) != edge_len_m1)
                    return MBError::READ_ERROR;
                key_buff = data.node_buff;
            } else {
                key_buff = p;
            }
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
        // Attach to cache on first use for readers/writers if available.
        PrefixCache* pc = dict.ActivePrefixCache();
        if (!pc)
            return false;

        PrefixCacheEntry entry;
        int n = pc->GetDepth(key, len, entry);
        if (n == 0)
            return false;

        // Stage all cursor updates and only commit on success
        const uint8_t* kcur = key_cursor;
        int lrem = len_remaining;
        int cons = consumed;

        // Use cached edge directly (shared and non-shared behave identically here).
        edge_ptrs.offset = entry.edge_offset;
        memcpy(edge_ptrs.edge_buff, entry.edge_buff, EDGE_SIZE);
        InitTempEdgePtrs(edge_ptrs);

        // Advance by the cached prefix length first
        kcur += n;
        lrem -= n;
        cons += n;

        // If this cached entry begins mid-edge (edge_skip > 0), finish the current
        // edge locally so that the subsequent traverseFromEdge() starts at a node
        // boundary. Verify remaining edge tail vs key.
        if (entry.edge_skip > 0) {
            int edge_len = edge_ptrs.len_ptr[0];
            int s = static_cast<int>(entry.edge_skip);
            if (s > edge_len)
                return false;
            int edge_len_m1 = edge_len - 1;
            const uint8_t* tail_ptr = nullptr;
            if (edge_len_m1 > 0) {
                if (loadEdgeKey(edge_ptrs, data, tail_ptr, edge_len_m1) != MBError::SUCCESS)
                    return false;
                int rem_tail = edge_len_m1 - (s - 1);
                if (rem_tail > 0) {
                    if (lrem < rem_tail)
                        return false;
                    if (memcmp(tail_ptr + (s - 1), kcur, rem_tail) != 0)
                        return false;
                    kcur += rem_tail;
                    lrem -= rem_tail;
                    cons += rem_tail;
                }
            }
        }

        // Commit staged cursor updates on success
        key_cursor = kcur;
        len_remaining = lrem;
        consumed = cons;
        return true;
    }

}
} // namespace mabain::detail
