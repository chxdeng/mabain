/**
 * Internal SearchEngine implementation consolidating rc-root/main-root logic
 * for exact, prefix, and lower-bound searches.
 */

#include "detail/search_engine.h"
#include "detail/lf_guard.h"
#include "dict.h"
#include "util/prefix_cache.h"
#include <cstdlib>
#include <cstring>
#include <time.h>

namespace mabain {
namespace detail {

	    

    int SearchEngine::find(const uint8_t* key, int len, MBData& data)
    {
        int rval;
        size_t rc_root_offset = dict.GetHeaderPtr()->rc_root_offset.load(MEMORY_ORDER_READER);
	        

        if (rc_root_offset != 0) {
            dict.reader_rc_off = rc_root_offset;
            rval = tryFindAtRoot(rc_root_offset, key, len, data);
            if (rval == MBError::SUCCESS) {
                data.match_len = len;
                return rval;
            } else if (rval != MBError::NOT_EXIST) {
                return rval;
            }
            data.options &= ~(CONSTS::OPTION_RC_MODE | CONSTS::OPTION_READ_SAVED_EDGE);
        } else {
            if (dict.reader_rc_off != 0) {
                dict.reader_rc_off = 0;
                dict.RemoveUnused(0);
                dict.mm.RemoveUnused(0);
            }
        }

        rval = tryFindAtRoot(0, key, len, data);
        if (rval == MBError::SUCCESS)
            data.match_len = len;

        return rval;
    }

    int SearchEngine::findPrefix(const uint8_t* key, int len, MBData& data)
    {
        int rval;
        MBData data_rc;
        size_t rc_root_offset = dict.GetHeaderPtr()->rc_root_offset.load(MEMORY_ORDER_READER);
        if (rc_root_offset != 0) {
            dict.reader_rc_off = rc_root_offset;
            rval = findPrefixInternal(rc_root_offset, key, len, data_rc);
#ifdef __LOCK_FREE__
            {
                int attempts = 0;
                while (rval == MBError::TRY_AGAIN && attempts++ < CONSTS::LOCK_FREE_RETRY_LIMIT) {
                    nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
                    data_rc.Clear();
                    rval = findPrefixInternal(rc_root_offset, key, len, data_rc);
                }
            }
#endif
            if (rval != MBError::NOT_EXIST && rval != MBError::SUCCESS)
                return rval;
            data.options &= ~(CONSTS::OPTION_RC_MODE | CONSTS::OPTION_READ_SAVED_EDGE);
        } else {
            if (dict.reader_rc_off != 0) {
                dict.reader_rc_off = 0;
                dict.RemoveUnused(0);
                dict.mm.RemoveUnused(0);
            }
        }

        rval = findPrefixInternal(0, key, len, data);
#ifdef __LOCK_FREE__
        {
            int attempts = 0;
            while (rval == MBError::TRY_AGAIN && attempts++ < CONSTS::LOCK_FREE_RETRY_LIMIT) {
                nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
                data.Clear();
                rval = findPrefixInternal(0, key, len, data);
            }
        }
#endif

        // The longer match wins.
        if (data_rc.match_len > data.match_len) {
            data_rc.TransferValueTo(data.buff, data.data_len);
            rval = MBError::SUCCESS;
        }
        return rval;
    }

    int SearchEngine::lowerBound(const uint8_t* key, int len, MBData& data, std::string* bound_key)
    {
        int rval;
        EdgePtrs& edge_ptrs = data.edge_ptrs;
        EdgePtrs bound_edge_ptrs;
        bound_edge_ptrs.curr_edge_index = -1;

        BoundSearchState bound_state {
            .key = key,
            .node_buff = data.node_buff,
            .bound_key = bound_key,
            .le_match_len = 0,
            .le_edge_key = -1,
            .use_curr_edge = false
        };

        int root_key = key[0];
        rval = lowerBoundCore(key, len, data, bound_key, bound_edge_ptrs, bound_state, root_key);

        if (rval == MBError::NOT_EXIST) {
            if (bound_state.use_curr_edge) {
                data.options &= ~CONSTS::OPTION_INTERNAL_NODE_BOUND;
                rval = readLowerBound(edge_ptrs, data, bound_key, -1);
            } else {
                if (bound_key != nullptr) {
                    bound_key->append((char*)key, bound_state.le_match_len);
                    if (data.options & CONSTS::OPTION_INTERNAL_NODE_BOUND) {
                        bound_state.le_edge_key = -1;
                    }
                }
                if (bound_edge_ptrs.curr_edge_index >= 0) {
                    InitTempEdgePtrs(bound_edge_ptrs);
                    rval = readLowerBound(bound_edge_ptrs, data, bound_key, bound_state.le_edge_key);
                } else {
                    rval = readBoundFromRootEdge(edge_ptrs, data, root_key, bound_key);
                }
            }
        } else if (rval == MBError::SUCCESS && bound_key) {
            bound_key->append(reinterpret_cast<const char*>(key), data.match_len);
        }

        return rval;
    }

    int SearchEngine::lowerBoundCore(const uint8_t* key, int len, MBData& data, std::string* bound_key,
        EdgePtrs& bound_edge_ptrs, BoundSearchState& bound_state, int root_key) const
    {
        EdgePtrs& edge_ptrs = data.edge_ptrs;

        // Always operate on the main root (no rc_root_offset diversion)
        int ret = dict.mm.GetRootEdge(0, root_key, edge_ptrs);
        if (ret != MBError::SUCCESS)
            return ret;
        if (edge_ptrs.len_ptr[0] == 0) {
            return readBoundFromRootEdge(edge_ptrs, data, root_key, bound_key);
        }

        const uint8_t* key_cursor = key;
        const uint8_t* edge_label_ptr = nullptr;
        int edge_len = edge_ptrs.len_ptr[0];
        int edge_label_len = edge_len - 1;

        int rval = MBError::NOT_EXIST;

        if (edge_len > LOCAL_EDGE_LEN) {
            size_t edge_label_off = Get5BInteger(edge_ptrs.ptr);
            if (dict.mm.ReadData(bound_state.node_buff, edge_label_len, edge_label_off) != edge_label_len)
                return MBError::READ_ERROR;
            edge_label_ptr = bound_state.node_buff;
        } else {
            edge_label_ptr = edge_ptrs.ptr;
        }

        if (edge_len < len) {
            int label_cmp = memcmp(edge_label_ptr, key_cursor + 1, edge_label_len);
            if (label_cmp != 0) {
                if (label_cmp < 0) {
                    bound_state.use_curr_edge = true;
                    if (bound_key) {
                        bound_key->push_back(static_cast<char>(key[0]));
                        bound_key->append(reinterpret_cast<const char*>(edge_label_ptr), edge_label_len);
                    }
                }
                return readBoundFromRootEdge(edge_ptrs, data, root_key, bound_key);
            }

            len -= edge_len;
            key_cursor += edge_len;
            data.match_len += edge_len;
            rval = traverseToLowerBound(key_cursor, len, edge_ptrs, data, bound_edge_ptrs, bound_state);
        } else if (edge_len == len) {
            if (len > 1 && memcmp(edge_label_ptr, key + 1, len - 1) != 0) {
                if (memcmp(edge_label_ptr, key + 1, len - 1) < 0) {
                    bound_state.use_curr_edge = true;
                    if (bound_key) {
                        bound_key->append(reinterpret_cast<const char*>(key), data.match_len + 1);
                        bound_key->append(reinterpret_cast<const char*>(edge_label_ptr), len - 1);
                    }
                }
            } else {
                rval = dict.ReadDataFromEdge(data, edge_ptrs);
                if (rval == MBError::SUCCESS)
                    data.match_len += edge_len;
            }
        }

        return rval;
    }

    int SearchEngine::findPrefixInternal(size_t root_off, const uint8_t* key, int len, MBData& data)
    {
        int rval;
        EdgePtrs& edge_ptrs = data.edge_ptrs;
#ifdef __LOCK_FREE__
        ReaderLFGuard lf_guard(dict.lfree, data);
#endif

        rval = dict.mm.GetRootEdge(root_off, key[0], edge_ptrs);
        if (rval != MBError::SUCCESS) {
            return MBError::READ_ERROR;
        }

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

        const uint8_t* key_buff;
        uint8_t* node_buff = data.node_buff;
        const uint8_t* key_cursor = key;
        int edge_len = edge_ptrs.len_ptr[0];
        int edge_len_m1 = edge_len - 1;
        if (edge_len > LOCAL_EDGE_LEN) {
            if (dict.mm.ReadData(node_buff, edge_len_m1, Get5BInteger(edge_ptrs.ptr))
                != edge_len_m1) {
#ifdef __LOCK_FREE__
                {
                    int _r = lf_guard.stop(edge_ptrs.offset);
                    if (_r != MBError::SUCCESS)
                        return _r;
                }
#endif
                return MBError::READ_ERROR;
            }
            key_buff = node_buff;
        } else {
            key_buff = edge_ptrs.ptr;
        }

        rval = MBError::NOT_EXIST;
        if (edge_len < len) {
            if (edge_len > 1 && memcmp(key_buff, key_cursor + 1, edge_len_m1) != 0) {
#ifdef __LOCK_FREE__
                {
                    int _r = lf_guard.stop(edge_ptrs.offset);
                    if (_r != MBError::SUCCESS)
                        return _r;
                }
#endif
                return MBError::NOT_EXIST;
            }

            len -= edge_len;
            key_cursor += edge_len;

            if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
#ifdef __LOCK_FREE__
                {
                    int _r = lf_guard.stop(edge_ptrs.offset);
                    if (_r != MBError::SUCCESS)
                        return _r;
                }
#endif
                data.match_len = key_cursor - key;
                return dict.ReadDataFromEdge(data, edge_ptrs);
            }

            uint8_t last_node_buffer[NODE_EDGE_KEY_FIRST];
            int last_prefix_rval = MBError::NOT_EXIST;
            rval = traversePrefixFromEdge(key, key_cursor, len, edge_ptrs, data, last_prefix_rval, last_node_buffer);
            if (rval == MBError::NOT_EXIST && last_prefix_rval != rval)
                rval = dict.ReadDataFromNode(data, last_node_buffer);
        } else if (edge_len == len) {
            if (edge_len_m1 == 0 || memcmp(key_buff, key + 1, edge_len_m1) == 0) {
                data.match_len = len;
                rval = dict.ReadDataFromEdge(data, edge_ptrs);
            }
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

    int SearchEngine::findInternal(size_t root_off, const uint8_t* key, int len, MBData& data)
    {
        EdgePtrs& edge_ptrs = data.edge_ptrs;
        int rval;
        const uint8_t* key_cursor = key;
        int orig_len = len;
        int consumed = 0;
        const uint8_t* key_buff;
        // Allow disabling prefix-cache usage for debugging via env var
        static int disable_pfx_cache = []() {
            const char* e = std::getenv("MB_DISABLE_PFXCACHE");
            return (e && *e && std::strcmp(e, "0") != 0) ? 1 : 0;
        }();
        // Do not seed from prefix cache for operations that require
        // precise parent/edge bookkeeping (e.g., remove). Stale mid-edge
        // cache entries can misguide traversal during structural updates.
        bool use_cache = !disable_pfx_cache
            && !(dict.reader_rc_off != 0 && root_off == dict.reader_rc_off)
            && !(data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT);
        bool used_cache = use_cache ? seedFromCache(key, len, edge_ptrs, data, key_cursor, len, consumed) : false;

        if (!used_cache) {
#ifdef __LOCK_FREE__
            ReaderLFGuard lf_guard(dict.lfree, data);
#endif
	            
            rval = dict.mm.GetRootEdge(root_off, key[0], edge_ptrs);
            if (rval != MBError::SUCCESS) {
                return MBError::READ_ERROR;
            }
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
                if (len <= 0) {
                    return resolveMatchOrInDict(data, edge_ptrs, true);
                }
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
                // Find does not update prefix cache; writer seeds during Add
            } else if (edge_len == len) {
                if (remainderMatches(key_buff, key_cursor, edge_len_m1)) {
                    // Find does not update prefix cache; writer seeds during Add
                    return resolveMatchOrInDict(data, edge_ptrs, true);
                }
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

    int SearchEngine::traverseFromEdge(const uint8_t*& key_cursor, int& len, int& consumed,
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
            if (data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT) {
                rval = dict.mm.NextEdge(key_cursor, edge_ptrs, node_buff, data);
            } else {
                int rf = dict.mm.NextEdgeFast(key_cursor, edge_ptrs, data);
                if (rf == MBError::INVALID_ARG) {
                    // Fallback to generic path if fast path not applicable
                    rval = dict.mm.NextEdge(key_cursor, edge_ptrs, node_buff, data);
                } else {
                    rval = rf;
                }
            }
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
            if (rval == MBError::NOT_EXIST)
                break;

            len -= edge_len;
            if (len <= 0) {
                // Find does not update prefix cache; writer seeds during Add
                int _ret = resolveMatchOrInDict(data, edge_ptrs, false);
                return _ret;
            }
            if (isLeaf(edge_ptrs)) {
                // Find does not update prefix cache; writer seeds during Add
                return MBError::NOT_EXIST;
            }

            key_cursor += edge_len;
            consumed += edge_len;
            // Find does not update prefix cache; writer seeds during Add
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

    int SearchEngine::traversePrefixFromEdge(const uint8_t* key_base, const uint8_t*& key_cursor, int& len,
        EdgePtrs& edge_ptrs, MBData& data, int& last_prefix_rval, uint8_t last_node_buffer[NODE_EDGE_KEY_FIRST]) const
    {
        int rval;
        uint8_t* node_buff = data.node_buff;
#ifdef __LOCK_FREE__
        ReaderLFGuard lf_guard(dict.lfree, data);
        size_t edge_offset_prev = edge_ptrs.offset;
#else
        size_t edge_offset_prev = edge_ptrs.offset;
#endif
        const uint8_t* key_buff;
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
            // Use fast path when possible (prefix find doesn't need parent bookkeeping)
            int rf = dict.mm.NextEdgeFast(key_cursor, edge_ptrs, data);
            if (rf == MBError::INVALID_ARG) {
                rval = dict.mm.NextEdge(key_cursor, edge_ptrs, node_buff, data);
            } else {
                rval = rf;
            }
            if (rval != MBError::READ_ERROR) {
                if (node_buff[0] & FLAG_NODE_MATCH) {
                    data.match_len = key_cursor - key_base;
                    memcpy(last_node_buffer, node_buff, NODE_EDGE_KEY_FIRST);
                    last_prefix_rval = MBError::SUCCESS;
                }
            }

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
            // match edge string: prefer direct pointer into mmap to avoid copy
            if (edge_len > LOCAL_EDGE_LEN) {
                size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
                const uint8_t* p = dict.mm.GetShmPtr(edge_str_off, edge_len_m1);
                if (p == nullptr) {
                    rval = MBError::READ_ERROR;
                    break;
                }
                key_buff = p;
            } else {
                key_buff = edge_ptrs.ptr;
            }

            // Compare remainder of the edge string when present and ensure edge is valid
            if (edge_len == 0 || (edge_len_m1 > 0 && memcmp(key_buff, key_cursor + 1, edge_len_m1) != 0)) {
                rval = MBError::NOT_EXIST;
                break;
            }

            len -= edge_len;
            key_cursor += edge_len;
            if (len <= 0 || (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
                data.match_len = key_cursor - key_base;
                rval = dict.ReadDataFromEdge(data, edge_ptrs);
                break;
            }
#ifdef __LOCK_FREE__
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

    void SearchEngine::appendEdgeKey(std::string* key, int edge_key, const EdgePtrs& edge_ptrs) const
    {
        key->push_back((char)edge_key);
        int edge_len_m1 = edge_ptrs.len_ptr[0] - 1;
        if (edge_len_m1 + 1 > LOCAL_EDGE_LEN) {
            size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
            uint8_t* edge_str_buff = dict.mm.GetShmPtr(edge_str_off, edge_len_m1);
            if (edge_str_buff != nullptr) {
                key->append((const char*)edge_str_buff, edge_len_m1);
            }
        } else if (edge_len_m1 > 0) {
            key->append(reinterpret_cast<const char*>(edge_ptrs.ptr), edge_len_m1);
        }
    }

    int SearchEngine::readLowerBound(EdgePtrs& edge_ptrs, MBData& data, std::string* bound_key, int le_edge_key) const
    {
        int rval;
        rval = dict.mm.ReadData(edge_ptrs.edge_buff, EDGE_SIZE, edge_ptrs.offset);
        if (rval != EDGE_SIZE)
            return MBError::READ_ERROR;

        rval = MBError::SUCCESS;
        int max_key = -1;
        while (!(edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
            if (bound_key != nullptr && le_edge_key >= 0) {
                appendEdgeKey(bound_key, le_edge_key, edge_ptrs);
                le_edge_key = -1;
            }
            max_key = -1;
            rval = dict.mm.NextMaxEdge(edge_ptrs, data.node_buff, data, max_key);
            if (rval != MBError::SUCCESS)
                break;
            le_edge_key = max_key;
        }

        if (bound_key != nullptr && le_edge_key >= 0 && (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
            appendEdgeKey(bound_key, le_edge_key, edge_ptrs);
        }

        if (rval == MBError::SUCCESS || rval == MBError::NOT_EXIST) {
            if (data.options & CONSTS::OPTION_KEY_ONLY) {
                // Key-only path: caller only needs bound position/key (bound_key handled by caller)
                return MBError::SUCCESS;
            }
            rval = dict.ReadDataFromEdge(data, edge_ptrs);
        }
        return rval;
    }

    int SearchEngine::readBoundFromRootEdge(EdgePtrs& edge_ptrs, MBData& data,
        int root_key, std::string* bound_key) const
    {
        int rval = MBError::NOT_EXIST;
        int ret;
        for (int i = root_key - 1; i >= 0; i--) {
            ret = dict.mm.GetRootEdge(0, i, edge_ptrs);
            if (ret != MBError::SUCCESS)
                return ret;
            if (edge_ptrs.len_ptr[0] != 0) {
                rval = readLowerBound(edge_ptrs, data, bound_key, i);
                break;
            }
        }
        return rval;
    }

    // Traverse along edges matching the target key while tracking the best
    // "<= key" fallback candidate on-the-fly. There is no explicit backtracking
    // in this loop: when a divergence is detected we return NOT_EXIST and let
    // the caller (lowerBound) jump to the saved candidate pointed by
    // bound_edge_ptrs or, if none, fall back to root scanning.
    //
    // Mechanics:
    // - NextLowerBoundEdge() does two things per step:
    //   1) Advances edge_ptrs to the exact next child (if present).
    //   2) Updates bound_edge_ptrs to the best lesser sibling (or records an
    //      internal-node match via OPTION_INTERNAL_NODE_BOUND). When a candidate
    //      is reported we also record its depth (state.le_match_len) and key
    //      (state.le_edge_key) so the caller can reconstruct bound_key if needed.
    // - After loading the edge label, we compare its tail (excluding the first
    //   discriminating byte) with the remaining input. If edge tail < suffix,
    //   we note use_curr_edge=true so the caller resolves lower-bound within the
    //   current subtree; otherwise we return and the caller uses the saved candidate.
    // - If we fully consume the input, or reach a data edge, we read and return.
    int SearchEngine::traverseToLowerBound(const uint8_t* key, int len, EdgePtrs& edge_ptrs,
        MBData& data, EdgePtrs& bound_edge_ptrs, BoundSearchState& state) const
    {
        const uint8_t* edge_label_ptr = nullptr;

        int steps = 0;
        while (true) {
            // Prevent pathological loops due to corruption or logic bugs.
            if (++steps > CONSTS::FIND_TRAVERSAL_LIMIT) {
                return MBError::UNKNOWN_ERROR;
            }
            // Ask memory layer to step to the exact child if available and to
            // update the best "less-than" candidate in bound_edge_ptrs.
            int candidate_le_key = -1;
            int status = dict.mm.NextLowerBoundEdge(key, len, edge_ptrs, state.node_buff, data, bound_edge_ptrs, candidate_le_key);

            // Record candidate metadata (depth/key) for bound_key reconstruction.
            if (state.bound_key && candidate_le_key >= 0) {
                state.le_match_len = data.match_len;
                state.le_edge_key = candidate_le_key;
            }

            // If no exact child, bail out and let the caller pivot to the candidate.
            if (status != MBError::SUCCESS)
                return status;

            int edge_len = edge_ptrs.len_ptr[0];
            int edge_label_len = edge_len - 1;

            // Load edge label tail (inline or overflow) for comparison.
            if (edge_len > LOCAL_EDGE_LEN) {
                size_t edge_label_off = Get5BInteger(edge_ptrs.ptr);
                if (dict.mm.ReadData(state.node_buff, edge_label_len, edge_label_off) != edge_label_len)
                    return MBError::READ_ERROR;
                edge_label_ptr = state.node_buff;
            } else {
                edge_label_ptr = edge_ptrs.ptr;
            }

            // Compare the remainder of the edge label with the remaining key bytes.
            // Any divergence returns NOT_EXIST to the caller, carrying the best
            // candidate captured so far in bound_edge_ptrs.
            if (edge_label_len > 0) {
                int label_cmp = memcmp(edge_label_ptr, key + 1, edge_label_len);
                if (label_cmp != 0) {
                    // If the edge label is strictly less than the key suffix, signal
                    // that the current subtree itself is a valid lower-bound pivot.
                    if (label_cmp < 0) {
                        state.use_curr_edge = true;
                        if (state.bound_key) {
                            state.bound_key->append(reinterpret_cast<const char*>(state.key), data.match_len + 1);
                            state.bound_key->append(reinterpret_cast<const char*>(edge_label_ptr), edge_label_len);
                        }
                    }
                    return MBError::NOT_EXIST;
                }
            } else if (edge_label_len < 0) {
                // Malformed edge; treat as not found.
                return MBError::NOT_EXIST;
            }

            // Advance. If we've consumed all input or landed on a data edge,
            // read and return the value.
            len -= edge_len;
            if (len <= 0) {
                status = dict.ReadDataFromEdge(data, edge_ptrs);
                if (status == MBError::SUCCESS)
                    data.match_len += edge_len;
                return status;
            }

            if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
                status = dict.ReadDataFromEdge(data, edge_ptrs);
                if (status == MBError::SUCCESS)
                    data.match_len += edge_len;
                return status;
            }

            // Continue traversal along the matched edge.
            key += edge_len;
            data.match_len += edge_len;
        }
    }

}
} // namespace mabain::detail
