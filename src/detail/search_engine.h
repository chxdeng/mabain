// Internal search engine to centralize search orchestration.
#pragma once

#include <cstdint>
#include <string>
#include <time.h>
#include "dict.h"
#include "detail/lf_guard.h"
#include "util/prefix_cache_iface.h"

namespace mabain {
class Dict;
struct MBData;

namespace detail {

class SearchEngine {
public:
    explicit SearchEngine(Dict& d) : dict(d) {}

    // Exact match
    int find(const uint8_t* key, int len, MBData& data);

    // Longest prefix match
    int findPrefix(const uint8_t* key, int len, MBData& data);

    // Lower bound (largest entry not greater than key)
    int lowerBound(const uint8_t* key, int len, MBData& data, std::string* bound_key);

    // Upper bound (first entry greater than key) — optional
    int upperBound(const uint8_t* key, int len, MBData& data, std::string* bound_key);

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
    int loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const;
    inline bool remainderMatches(const uint8_t* key_buff, const uint8_t* p, int rem_len) const;
    inline bool isLeaf(const EdgePtrs& edge_ptrs) const;
    inline int compareCurrEdgeTail(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t* p,
        const uint8_t*& key_buff, int& edge_len, int& edge_len_m1) const;
    inline int resolveMatchOrInDict(MBData& data, EdgePtrs& edge_ptrs, bool at_root) const;
    // Fast root-edge accessor with tiny thread-local cache when no writers
    inline int getRootEdgeFast(size_t root_off, int nt, EdgePtrs& edge_ptrs) const;
    inline bool seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
        MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const;
    inline void maybePutCache(const uint8_t* full_key, int full_len, int consumed,
        const EdgePtrs& edge_ptrs) const;
    // declared once above

    // Lower-bound internals
    void appendEdgeKey(std::string* key, int edge_key, const EdgePtrs& edge_ptrs) const;
    int readLowerBound(EdgePtrs& edge_ptrs, MBData& data, std::string* bound_key, int le_edge_key) const;
    int readBoundFromRootEdge(EdgePtrs& edge_ptrs, MBData& data, int root_key, std::string* bound_key) const;
    int traverseToLowerBound(const uint8_t* key, int len, EdgePtrs& edge_ptrs, MBData& data,
        EdgePtrs& bound_edge_ptrs, BoundSearchState& state) const;
};

} // namespace detail
} // namespace mabain

// Inline implementations for hot exact-match path
namespace mabain { namespace detail {

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

inline int SearchEngine::findInternal(size_t root_off, const uint8_t* key, int len, MBData& data)
{
    EdgePtrs& edge_ptrs = data.edge_ptrs;
    int rval;
    const uint8_t* key_cursor = key;
    int orig_len = len;
    int consumed = 0;
    const uint8_t* key_buff;
    bool use_cache = !(dict.reader_rc_off != 0 && root_off == dict.reader_rc_off);
    bool used_cache = use_cache ? seedFromCache(key, len, edge_ptrs, data, key_cursor, len, consumed) : false;

    if (!used_cache) {
#ifdef __LOCK_FREE__
        ReaderLFGuard lf_guard(dict.lfree, data);
#endif
        rval = getRootEdgeFast(root_off, key[0], edge_ptrs);
        if (rval != MBError::SUCCESS)
            return MBError::READ_ERROR;
        if (edge_ptrs.len_ptr[0] == 0) {
#ifdef __LOCK_FREE__
            {
                int _r = lf_guard.stop(edge_ptrs.offset);
                if (_r != MBError::SUCCESS)
                    return _r;
            }
#endif
            return MBError::NOT_EXIST;
        }

        int edge_len = edge_ptrs.len_ptr[0];
        int edge_len_m1 = edge_len - 1;
        rval = MBError::NOT_EXIST;
        if ((rval = loadEdgeKey(edge_ptrs, data, key_buff, edge_len_m1)) != MBError::SUCCESS) {
#ifdef __LOCK_FREE__
            {
                int _r = lf_guard.stop(edge_ptrs.offset);
                if (_r != MBError::SUCCESS)
                    return _r;
            }
#endif
            return MBError::READ_ERROR;
        }

        if (edge_len < len) {
            if (!remainderMatches(key_buff, key_cursor, edge_len_m1)) {
#ifdef __LOCK_FREE__
                {
                    int _r = lf_guard.stop(edge_ptrs.offset);
                    if (_r != MBError::SUCCESS)
                        return _r;
                }
#endif
                return MBError::NOT_EXIST;
            }
            key_cursor += edge_len;
            consumed += edge_len;
            len -= edge_len;
            if (len <= 0)
                return resolveMatchOrInDict(data, edge_ptrs, true);
            if (isLeaf(edge_ptrs)) {
#ifdef __LOCK_FREE__
                {
                    int _r = lf_guard.stop(edge_ptrs.offset);
                    if (_r != MBError::SUCCESS)
                        return _r;
                }
#endif
                return MBError::NOT_EXIST;
            }
            maybePutCache(key, orig_len, consumed, edge_ptrs);
        } else if (edge_len == len) {
            if (remainderMatches(key_buff, key_cursor, edge_len_m1))
                return resolveMatchOrInDict(data, edge_ptrs, true);
#ifdef __LOCK_FREE__
            {
                int _r = lf_guard.stop(edge_ptrs.offset);
                if (_r != MBError::SUCCESS)
                    return _r;
            }
#endif
            return MBError::NOT_EXIST;
        } else {
#ifdef __LOCK_FREE__
            {
                int _r = lf_guard.stop(edge_ptrs.offset);
                if (_r != MBError::SUCCESS)
                    return _r;
            }
#endif
            return MBError::NOT_EXIST;
        }

#ifdef __LOCK_FREE__
        {
            int _r = lf_guard.stop(edge_ptrs.offset);
            if (_r != MBError::SUCCESS)
                return _r;
        }
#endif
    }

    if (used_cache) {
        if (len <= 0)
            return resolveMatchOrInDict(data, edge_ptrs, false);
        if (isLeaf(edge_ptrs))
            return MBError::NOT_EXIST;
    }
    return traverseFromEdge(key_cursor, len, consumed, key, orig_len, edge_ptrs, data);
}

inline int SearchEngine::traverseFromEdge(const uint8_t*& key_cursor, int& len, int& consumed,
    const uint8_t* full_key, int full_len, EdgePtrs& edge_ptrs, MBData& data)
{
    const uint8_t* key_buff;
    uint8_t* node_buff = data.node_buff;
    int rval = MBError::SUCCESS;
#ifdef __LOCK_FREE__
    ReaderLFGuard lf_guard(dict.lfree, data);
    size_t edge_offset_prev = edge_ptrs.offset;
#else
    size_t edge_offset_prev = edge_ptrs.offset;
#endif
    int steps = 0;
    while (true) {
        if (++steps > CONSTS::FIND_TRAVERSAL_LIMIT) {
#ifdef __LOCK_FREE__
            {
                int _r = lf_guard.stop(edge_ptrs.offset);
                if (_r != MBError::SUCCESS)
                    return _r;
            }
#endif
            return MBError::UNKNOWN_ERROR;
        }
        rval = dict.mm.NextEdge(key_cursor, edge_ptrs, node_buff, data);
        if (rval != MBError::SUCCESS)
            break;

#ifdef __LOCK_FREE__
        {
            int _r = lf_guard.stop(edge_offset_prev);
            if (_r != MBError::SUCCESS)
                return _r;
        }
#endif
        int edge_len = edge_ptrs.len_ptr[0];
        int edge_len_m1 = edge_len - 1;
        rval = compareCurrEdgeTail(edge_ptrs, data, key_cursor, key_buff, edge_len, edge_len_m1);
        if (rval == MBError::READ_ERROR)
            break;
        if (rval == MBError::NOT_EXIST) {
            rval = MBError::NOT_EXIST;
            break;
        }

        len -= edge_len;
        if (len <= 0) {
            rval = resolveMatchOrInDict(data, edge_ptrs, false);
            break;
        }
        if (isLeaf(edge_ptrs)) {
            rval = MBError::NOT_EXIST;
            break;
        }

        key_cursor += edge_len;
        consumed += edge_len;
        maybePutCache(full_key, full_len, consumed, edge_ptrs);
#ifdef __LOCK_FREE__
        edge_offset_prev = edge_ptrs.offset;
#else
        edge_offset_prev = edge_ptrs.offset;
#endif
    }

#ifdef __LOCK_FREE__
    {
        int _r = lf_guard.stop(edge_ptrs.offset);
        if (_r != MBError::SUCCESS)
            return _r;
    }
#endif
    return rval;
}

} } // namespace mabain::detail

// Additional small inline helpers to avoid extra call overhead
namespace mabain { namespace detail {

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
inline int SearchEngine::getRootEdgeFast(size_t root_off, int nt, EdgePtrs& edge_ptrs) const
{
    struct RootEdgeCacheEntry {
        size_t base;
        uint8_t edge_buff[EDGE_SIZE];
        bool valid;
    };
    // One slot per first-byte key
    thread_local static RootEdgeCacheEntry cache[NUM_ALPHABET];

    const bool no_writers = dict.GetHeaderPtr()->num_writer == 0;
    const size_t base = (root_off != 0) ? root_off : dict.mm.GetRootOffset();

    RootEdgeCacheEntry& slot = cache[nt];
    if (no_writers && slot.valid && slot.base == base) {
        // Fill EdgePtrs from cached edge buffer
        // Compute offset exactly as DictMem::GetRootEdge would
        edge_ptrs.offset = base + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt * EDGE_SIZE;
        memcpy(edge_ptrs.edge_buff, slot.edge_buff, EDGE_SIZE);
        InitTempEdgePtrs(edge_ptrs);
        return MBError::SUCCESS;
    }

    int ret = dict.mm.GetRootEdge(root_off, nt, edge_ptrs);
    if (ret != MBError::SUCCESS)
        return ret;

    if (no_writers) {
        slot.base = base;
        memcpy(slot.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
        slot.valid = true;
    }
    return MBError::SUCCESS;
}

inline int SearchEngine::loadEdgeKey(const EdgePtrs& edge_ptrs, MBData& data, const uint8_t*& key_buff, int edge_len_m1) const
{
    if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
        size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
        // Try to get a direct pointer to shared memory to avoid a copy
        uint8_t* shm_ptr = dict.mm.GetShmPtr(edge_str_off, edge_len_m1);
        if (shm_ptr != nullptr) {
            key_buff = shm_ptr;
        } else {
            if (dict.mm.ReadData(data.node_buff, edge_len_m1, edge_str_off) != edge_len_m1)
                return MBError::READ_ERROR;
            key_buff = data.node_buff;
        }
    } else {
        key_buff = edge_ptrs.ptr;
    }
    return MBError::SUCCESS;
}

inline bool SearchEngine::seedFromCache(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
    MBData& data, const uint8_t*& key_cursor, int& len_remaining, int& consumed) const
{
    PrefixCacheIface* pc = dict.ActivePrefixCache();
    if (!pc) return false;

    PrefixCacheEntry entry;
    const int n = pc->PrefixLen();
    if (len < n || !pc->Get(key, len, entry))
        return false;

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

} } // namespace mabain::detail
