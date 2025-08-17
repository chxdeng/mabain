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
        inline bool seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
            MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const;
        inline void maybePutCache(const uint8_t* full_key, int full_len, int consumed,
            const EdgePtrs& edge_ptrs) const;
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
        return dict.ReadDataFromEdge(data, edge_ptrs);
    }

    // Tiny per-thread cache of root edges to avoid a ReadData on hot prefixes.
    // Safe only when no writers are active; guarded by header->num_writer == 0.
    // (getRootEdgeFast removed; callers read via dict.mm.GetRootEdge)

    inline int SearchEngine::loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const
    {
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
            if (dict.mm.ReadData(data.node_buff, edge_len_m1, edge_str_off) != edge_len_m1)
                return MBError::READ_ERROR;
            key_buff = data.node_buff;
        } else {
            key_buff = edge_ptrs.ptr;
        }
        return MBError::SUCCESS;
    }

    inline bool SearchEngine::seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
        MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const
    {
        PrefixCacheIface* pc = dict.ActivePrefixCache();
        if (!pc)
            return false;

        PrefixCacheEntry entry;
        const int n = pc->PrefixLen();
        if (pc->IsShared()) {
            if (len < n || !pc->Get(key, len, entry))
                return false;
        } else {
            // Non-shared: allow matching any prefix length m where m <= n
            bool got = false;
            const int max_m = (len < n ? len : n);
            for (int m = max_m; m >= 1; --m) {
                if (pc->Get(key, m, entry)) { got = true; break; }
            }
            if (!got) return false;
            // Adjust 'n' to the actual consumed length we matched
            // We set key_cursor and counters with the matched prefix length.
            // Note: entry was produced using m, but we don't store m; we only advance by the length used in Get.
            // To retrieve 'm', we re-check by decrementing until match; keep the value we found in loop.
            // To keep it simple, re-run a quick loop to compute matched m equivalently.
            int matched = 0;
            for (int m = max_m; m >= 1; --m) {
                PrefixCacheEntry tmp;
                if (pc->Get(key, m, tmp)) { matched = m; break; }
            }
            if (matched <= 0) return false;
            // Use matched as n for advancing below
            if (matched < n) {
                // Shadow 'n' by updating len_remaining/consumed using matched below
            }
            // Set advancement based on 'matched'
            key_cursor = key + matched;
            len_remaining -= matched;
            consumed += matched;
            // Fill edge_ptrs from entry and return early
            if (pc->IsShared() && dict.GetHeaderPtr() && dict.GetHeaderPtr()->num_writer > 0) {
                // Shared revalidate when writers active
                uint8_t curr_edge[EDGE_SIZE];
                if (dict.mm.ReadData(curr_edge, EDGE_SIZE, entry.edge_offset) != EDGE_SIZE)
                    return false;
                if (memcmp(curr_edge, entry.edge_buff, EDGE_SIZE) != 0)
                    return false;
            }
            edge_ptrs.offset = entry.edge_offset;
            memcpy(edge_ptrs.edge_buff, entry.edge_buff, EDGE_SIZE);
            InitTempEdgePtrs(edge_ptrs);
            data.options |= CONSTS::OPTION_READ_SAVED_EDGE;
            return true;
        }

        if (pc->IsShared() && dict.GetHeaderPtr() && dict.GetHeaderPtr()->num_writer > 0) {
            uint8_t curr_edge[EDGE_SIZE];
            if (dict.mm.ReadData(curr_edge, EDGE_SIZE, entry.edge_offset) != EDGE_SIZE) {
                return false;
            }
            if (memcmp(curr_edge, entry.edge_buff, EDGE_SIZE) != 0) {
                return false;
            }
        }

        edge_ptrs.offset = entry.edge_offset;
        memcpy(edge_ptrs.edge_buff, entry.edge_buff, EDGE_SIZE);
        InitTempEdgePtrs(edge_ptrs);
        data.options |= CONSTS::OPTION_READ_SAVED_EDGE;

        key_cursor = key + n;
        len_remaining -= n;
        consumed += n;
        return true;
    }

    inline void SearchEngine::maybePutCache(const uint8_t* full_key, int full_len, int consumed,
        const EdgePtrs& edge_ptrs) const
    {
        dict.MaybePutCache(full_key, full_len, consumed, edge_ptrs);
    }

}
} // namespace mabain::detail
