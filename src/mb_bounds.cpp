/**
 * Copyright (C) 2025 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @author Changxue Deng <chadeng@cisco.com>

#include "dict.h"

namespace mabain {

int Dict::ReadLowerBound(EdgePtrs& edge_ptrs, MBData& data, std::string* bound_key, int le_edge_key) const
{
    int rval;
    rval = mm.ReadData(edge_ptrs.edge_buff, EDGE_SIZE, edge_ptrs.offset);
    if (rval != EDGE_SIZE)
        return MBError::READ_ERROR;

    rval = MBError::SUCCESS;
    int max_key = -1;
    while (!(edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
        if (bound_key != nullptr && le_edge_key >= 0) {
            bound_key->push_back((char)le_edge_key);
            le_edge_key = -1;

            int edge_len_m1 = edge_ptrs.len_ptr[0] - 1;
            if (edge_len_m1 + 1 > LOCAL_EDGE_LEN) {
                size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
                uint8_t* edge_str_buff = mm.GetShmPtr(edge_str_off, edge_len_m1);
                if (edge_str_buff != nullptr) {
                    bound_key->append((char*)edge_str_buff, edge_len_m1);
                } else {
                    return MBError::READ_ERROR;
                }
            } else if (edge_len_m1 > 0) {
                bound_key->append(reinterpret_cast<const char*>(edge_ptrs.ptr), edge_len_m1);
            }
        }
        max_key = -1;
        // Read the next maximum edge
        rval = mm.NextMaxEdge(edge_ptrs, data.node_buff, data, max_key);
        if (rval != MBError::SUCCESS)
            break;
        le_edge_key = max_key;
    }

    if (bound_key != nullptr && le_edge_key >= 0 && (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
        bound_key->push_back((char)le_edge_key);
        if (true) {
            int edge_len_m1 = edge_ptrs.len_ptr[0] - 1;
            if (edge_len_m1 + 1 > LOCAL_EDGE_LEN) {
                size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
                uint8_t* edge_str_buff = mm.GetShmPtr(edge_str_off, edge_len_m1);
                if (edge_str_buff != nullptr) {
                    bound_key->append((char*)edge_str_buff, edge_len_m1);
                } else {
                    return MBError::READ_ERROR;
                }
            } else if (edge_len_m1 > 0) {
                bound_key->append(reinterpret_cast<const char*>(edge_ptrs.ptr), edge_len_m1);
            }
        }
    }

    if (rval == MBError::SUCCESS || rval == MBError::NOT_EXIST)
        rval = ReadDataFromEdge(data, edge_ptrs);
    return rval;
}

int Dict::ReadDataFromBoundEdge(bool use_curr_edge, EdgePtrs& edge_ptrs,
    EdgePtrs& bound_edge_ptrs, MBData& data,
    int root_key, std::string* bound_key, int le_edge_key) const
{
    int rval = MBError::NOT_EXIST;
    if (use_curr_edge) {
        data.options &= ~CONSTS::OPTION_INTERNAL_NODE_BOUND;
        rval = ReadLowerBound(edge_ptrs, data, bound_key, le_edge_key);
    } else if (bound_edge_ptrs.curr_edge_index >= 0) {
        InitTempEdgePtrs(bound_edge_ptrs);
        rval = ReadLowerBound(bound_edge_ptrs, data, bound_key, le_edge_key);
    } else {
        int ret;
        // check for root edge (edge_ptrs still points to root edge)
        for (int i = root_key - 1; i >= 0; i--) {
            ret = mm.GetRootEdge(0, i, edge_ptrs);
            if (ret != MBError::SUCCESS)
                return ret;
            if (edge_ptrs.len_ptr[0] != 0) {
                rval = ReadLowerBound(edge_ptrs, data, bound_key, i);
                break;
            }
        }
    }

    return rval;
}

int Dict::FindBound(size_t root_off, const uint8_t* key, int len, MBData& data, std::string* bound_key)
{
    EdgePtrs& edge_ptrs = data.edge_ptrs;
    EdgePtrs bound_edge_ptrs;
    bound_edge_ptrs.curr_edge_index = -1;
    bool use_curr_edge = false;
    int root_key = key[0];

    int rval = mm.GetRootEdge(root_off, key[0], edge_ptrs);
    if (rval != MBError::SUCCESS)
        return rval;
    if (edge_ptrs.len_ptr[0] == 0) {
        return ReadDataFromBoundEdge(use_curr_edge, edge_ptrs, bound_edge_ptrs,
            data, root_key, bound_key, -1);
    }

    int key_cmp;
    const uint8_t* key_buff;
    uint8_t* node_buff = data.node_buff;
    const uint8_t* p = key;
    int edge_len = edge_ptrs.len_ptr[0];
    int edge_len_m1 = edge_len - 1;
    rval = MBError::NOT_EXIST;
    if (edge_len > LOCAL_EDGE_LEN) {
        size_t edge_str_off_lf = Get5BInteger(edge_ptrs.ptr);
        if (mm.ReadData(node_buff, edge_len_m1, edge_str_off_lf) != edge_len_m1)
            return MBError::READ_ERROR;
        key_buff = node_buff;
    } else {
        key_buff = edge_ptrs.ptr;
    }

    int le_match_len = 0;
    int le_edge_key = -1;
    if (edge_len < len) {
        key_cmp = memcmp(key_buff, p + 1, edge_len_m1);
        if (key_cmp != 0) {
            if (key_cmp < 0) {
                use_curr_edge = true;
                if (bound_key != nullptr) {
                    bound_key->push_back((char)key[0]);
                    bound_key->append((char*)key_buff, edge_len_m1);
                }
            }
            return ReadDataFromBoundEdge(use_curr_edge, edge_ptrs, bound_edge_ptrs, data, root_key, bound_key, -1);
        }

        len -= edge_len;
        p += edge_len;
        data.match_len += edge_len;
        while (true) {
            int new_le_key = -1;
            rval = mm.NextLowerBoundEdge(p, len, edge_ptrs, node_buff, data, bound_edge_ptrs, new_le_key);
            if (bound_key != nullptr && new_le_key >= 0) {
                le_match_len = data.match_len;
                le_edge_key = new_le_key;
            }
            if (rval != MBError::SUCCESS)
                break;

            edge_len = edge_ptrs.len_ptr[0];
            edge_len_m1 = edge_len - 1;
            // match edge string
            if (edge_len > LOCAL_EDGE_LEN) {
                size_t edge_str_off_lf = Get5BInteger(edge_ptrs.ptr);
                if (mm.ReadData(node_buff, edge_len_m1, edge_str_off_lf) != edge_len_m1) {
                    rval = MBError::READ_ERROR;
                    break;
                }
                key_buff = node_buff;
            } else {
                key_buff = edge_ptrs.ptr;
            }

            if (edge_len_m1 > 0) {
                key_cmp = memcmp(key_buff, p + 1, edge_len_m1);
                if (key_cmp != 0) {
                    if (key_cmp < 0) {
                        use_curr_edge = true;
                        if (bound_key != nullptr) {
                            bound_key->append((char*)key, data.match_len + 1);
                            bound_key->append((char*)key_buff, edge_len_m1);
                        }
                    }
                    rval = MBError::NOT_EXIST;
                    break;
                }
            } else if (edge_len_m1 < 0) {
                rval = MBError::NOT_EXIST;
                break;
            }

            len -= edge_len;
            if (len <= 0) {
                rval = ReadDataFromEdge(data, edge_ptrs);
                if (rval == MBError::SUCCESS)
                    data.match_len += edge_len;
                break;
            } else {
                if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
                    // Reach a leaf node and no match found
                    // This must be the lower bound.
                    rval = ReadDataFromEdge(data, edge_ptrs);
                    if (rval == MBError::SUCCESS)
                        data.match_len += edge_len;
                    break;
                }
            }
            p += edge_len;
            data.match_len += edge_len;
        }
    } else if (edge_len == len) {
        if (len > 1 && (key_cmp = memcmp(key_buff, key + 1, len - 1)) != 0) {
            if (key_cmp < 0) {
                use_curr_edge = true;
                if (bound_key != nullptr) {
                    bound_key->append((char*)key, data.match_len + 1);
                    bound_key->append((char*)key_buff, len - 1);
                }
            }
        } else {
            rval = ReadDataFromEdge(data, edge_ptrs);
            if (rval == MBError::SUCCESS)
                data.match_len += edge_len;
        }
    }

    if (rval == MBError::NOT_EXIST) {
        if (bound_key != nullptr) {
            if (!use_curr_edge) {
                bound_key->append((char*)key, le_match_len);
                if (le_edge_key >= 0 && !(data.options & CONSTS::OPTION_INTERNAL_NODE_BOUND)) {
                } else {
                    le_edge_key = -1;
                }
            } else {
                le_edge_key = -1;
            }
        }
        rval = ReadDataFromBoundEdge(use_curr_edge, edge_ptrs, bound_edge_ptrs, data, root_key, bound_key, le_edge_key);
    } else if (rval == MBError::SUCCESS) {
        if (bound_key != nullptr) {
            bound_key->append((char*)key, data.match_len);
        }
    }

    return rval;
}

}
