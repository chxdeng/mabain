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

#include <errno.h>
#include <iostream>
#include <stdlib.h>

#include "async_writer.h"
#include "db.h"
#include "detail/search_engine.h"
#include "dict.h"
#include "dict_mem.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "mabain_consts.h"
#include "util/prefix_cache.h"

#define DATA_HEADER_SIZE 32

namespace mabain {

Dict::Dict(const std::string& mbdir, bool init_header, int datasize,
    int db_options, size_t memsize_index, size_t memsize_data,
    uint32_t block_sz_idx, uint32_t block_sz_data,
    int max_num_index_blk, int max_num_data_blk,
    int64_t entry_per_bucket, uint32_t queue_size,
    const char* queue_dir)
    : DRMBase(mbdir, db_options, false)
    , mm(mbdir, init_header, memsize_index, db_options, block_sz_idx, max_num_index_blk, queue_size)
    , queue(NULL)
{
    status = MBError::NOT_INITIALIZED;
    reader_rc_off = 0;
    slaq = NULL;

    header = mm.GetHeaderPtr();
    if (header == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "header not mapped");
        throw (int)MBError::MMAP_FAILED;
    }

    if (!init_header) {
        // confirm block size is the same
        if (block_sz_data != 0 && header->data_block_size != block_sz_data) {
            std::cerr << "mabain data block size not match " << block_sz_data << ": "
                      << header->data_block_size << std::endl;
            PrintHeader(std::cout);
            Destroy();
            throw (int)MBError::INVALID_SIZE;
        }
    } else {
        header->data_block_size = block_sz_data;
    }

    if (!(db_options & CONSTS::READ_ONLY_DB)) {
        // initialize shared memory queue
        slaq = qmgr.CreateFile(header->shm_queue_id, queue_size, queue_dir, db_options);
        queue = slaq->queue;
    }
    lfree.LockFreeInit(&header->lock_free, header, db_options);
    mm.InitLockFreePtr(&lfree);
    mbp = MBPipe(mbdir, 0);
    mbdir_ = mbdir;

    // Open data file
    kv_file = new RollableFile(mbdir + "_mabain_d",
        static_cast<size_t>(header->data_block_size),
        memsize_data, db_options, max_num_data_blk);

    // If init_header is false, we can set the dict status to SUCCESS.
    // Otherwise, the status will be set in the Init.
    if (init_header) {
        // Initialize header
        // We known that only writers will set init_header to true.
        header->entry_per_bucket = entry_per_bucket;
        header->index_block_size = block_sz_idx;
        header->data_block_size = block_sz_data;
        header->data_size = datasize;
        header->count = 0;
        // Initialize embedded prefix cache layout only if option is set.
        // Otherwise, do not reserve space; readers will self-check consistency.
        if (options & CONSTS::OPTION_PREFIX_CACHE) {
            InitEmbeddedPrefixCacheLayout();
        } else {
            header->pfxcache_offset = 0;
            header->pfxcache_size = 0;
            header->pfx_cap2 = header->pfx_cap3 = header->pfx_cap4 = 0;
            header->m_data_offset = DATA_HEADER_SIZE; // start from a non-zero offset
        }
    } else {
        if (options & CONSTS::ACCESS_MODE_WRITER) {
            if (header->entry_per_bucket != entry_per_bucket) {
                std::cerr << "mabain count per bucket not match\n";
            }
            // Check if jemalloc option is conststent
            if ((options & CONSTS::OPTION_JEMALLOC) && !(header->writer_options & CONSTS::OPTION_JEMALLOC)) {
                std::cerr << "mabain jemalloc option not match\n";
                Destroy();
                throw (int)MBError::INVALID_ARG;
            }
            if (!(options & CONSTS::OPTION_JEMALLOC) && (header->writer_options & CONSTS::OPTION_JEMALLOC)) {
                std::cerr << "mabain jemalloc option not match header\n";
                Destroy();
                throw (int)MBError::INVALID_ARG;
            }
        }
        // Self-consistency: if DB lacks embedded cache region, ignore option.
        if (!(header->pfxcache_size > 0) && (options & CONSTS::OPTION_PREFIX_CACHE)) {
            std::cerr << "Prefix cache option set but DB has no embedded cache; disabling.\n";
            options &= ~CONSTS::OPTION_PREFIX_CACHE;
        }
    }

    // In jemalloc mode, ensure the data-file arena starts after the reserved
    // embedded prefix-cache region so allocations do not overwrite cache tables.
    if ((options & CONSTS::OPTION_JEMALLOC) && header->pfxcache_size > 0) {
        // header->m_data_offset already points to the first user-data byte.
        // PreAlloc advances the arena's alloc_size to this offset for block 0.
        (void)kv_file->PreAlloc(header->m_data_offset);
    }
    // Instantiate the prefix cache only if the DB has an embedded cache region
    // and the caller requested prefix cache via OPTION_PREFIX_CACHE.
    if (!(options & CONSTS::ASYNC_WRITER_MODE)) {
        bool want_cache = (header->pfxcache_size > 0) && (options & CONSTS::OPTION_PREFIX_CACHE);
        if (want_cache) {
            try {
                prefix_cache = std::unique_ptr<PrefixCache>(new PrefixCache(mbdir_, header, /*capacity hint*/65536));
            } catch (...) {
                // Leave cache disabled on failure; DB remains operational.
            }
        }
    }
    if (mm.IsValid())
        status = MBError::SUCCESS;
}

// keep namespace open for all method definitions below

Dict::~Dict()
{
}

// Compute and set embedded prefix-cache layout in the data header.
// This keeps calculations localized and avoids cluttering the constructor.
void Dict::InitEmbeddedPrefixCacheLayout()
{
    // Choose capacities: full 2-byte coverage and modest 3/4 byte tables
    auto compute_floor_pow2 = [](size_t x) -> size_t {
        if (x == 0)
            return 0;
        size_t p = 1;
        while ((p << 1) && ((p << 1) <= x))
            p <<= 1;
        return p;
    };

    size_t cap2 = 65536;                      // full 2-byte coverage
    size_t cap3 = compute_floor_pow2(1u << 18); // 256K entries
    size_t cap4 = compute_floor_pow2(1u << 16); // 64K entries

    auto calc_pfx_bytes = [&](size_t c2, size_t c3, size_t c4) -> size_t {
        size_t t2 = sizeof(PrefixCacheEntry) * (c2 ? c2 : 1);
        size_t g2 = sizeof(uint32_t) * (c2 ? c2 : 1);
        size_t t3 = sizeof(PrefixCacheEntry) * (c3 ? c3 : 1);
        size_t g3 = sizeof(uint32_t) * (c3 ? c3 : 1);
        size_t t4 = sizeof(PrefixCacheEntry) * (c4 ? c4 : 1);
        size_t g4 = sizeof(uint32_t) * (c4 ? c4 : 1);
        size_t v4 = sizeof(uint32_t) * (c4 ? c4 : 1);
        // PCShmHeader: magic(u32) + version(u16) + reserved(u16) + 6*u32(caps/masks)
        return sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t) + 6 * sizeof(uint32_t)
            + g2 + t2 + g3 + t3 + v4 + g4 + t4;
    };

    size_t pfx_bytes = calc_pfx_bytes(cap2, cap3, cap4);
    size_t pfx_off = DATA_HEADER_SIZE; // place right after the small header
    size_t block0 = header->data_block_size;

    header->pfxcache_offset = 0;
    header->pfxcache_size = 0;
    header->pfx_cap2 = header->pfx_cap3 = header->pfx_cap4 = 0;
    header->m_data_offset = DATA_HEADER_SIZE;

    if (block0 > 0) {
        while (pfx_off + pfx_bytes > block0) {
            if (cap4 > 0)
                cap4 >>= 1;
            else if (cap3 > 0)
                cap3 >>= 1;
            else
                break;
            pfx_bytes = calc_pfx_bytes(cap2, cap3, cap4);
        }

        if (pfx_off + pfx_bytes <= block0) {
            header->pfxcache_offset = pfx_off;
            header->pfxcache_size = pfx_bytes;
            header->pfx_cap2 = static_cast<uint32_t>(cap2);
            header->pfx_cap3 = static_cast<uint32_t>(cap3);
            header->pfx_cap4 = static_cast<uint32_t>(cap4);
            header->m_data_offset = header->pfxcache_offset + header->pfxcache_size;
        }
    }
}

// This function only needs to be called by writer.
int Dict::Init(uint32_t id)
{
    Logger::Log(LOG_LEVEL_DEBUG, "connector %u initializing db", id);
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        Logger::Log(LOG_LEVEL_ERROR, "dict initialization not allowed for non-writer");
        return MBError::NOT_ALLOWED;
    }

    if (status != MBError::NOT_INITIALIZED) {
        // status can be NOT_INITIALIZED or SUCCESS.
        Logger::Log(LOG_LEVEL_WARN, "connector %u dict already initialized", id);
        return MBError::SUCCESS;
    }

    if (header == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "connector %u header not mapped", id);
        return MBError::ALLOCATION_ERROR;
    }

    Logger::Log(LOG_LEVEL_DEBUG, "connector %u initializing DictMem", id);
    mm.InitRootNode();

    if (header->data_size > CONSTS::MAX_DATA_SIZE) {
        Logger::Log(LOG_LEVEL_ERROR, "data size %d is too large", header->data_size);
        return MBError::INVALID_SIZE;
    }

    if (mm.IsValid())
        status = MBError::SUCCESS;

    return status;
}

void Dict::Destroy()
{
    mm.Destroy();

    if (free_lists != NULL)
        delete free_lists;

    if (kv_file != NULL)
        delete kv_file;
}

int Dict::Status() const
{
    return status;
}

// DB calls SearchEngine directly for lookups.

// Add a key-value pair
// if overwrite is true and an entry with input key already exists, the old data will
// be overwritten. Otherwise, IN_DICT will be returned.
int Dict::Add(const uint8_t* key, int len, MBData& data, bool overwrite)
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        return MBError::NOT_ALLOWED;
    }
    if (len > CONSTS::MAX_KEY_LENGHTH || data.data_len > CONSTS::MAX_DATA_SIZE || len <= 0 || data.data_len <= 0)
        return MBError::OUT_OF_BOUND;

    EdgePtrs edge_ptrs;
    int rval;

    rval = mm.GetRootEdge_Writer(data.options & CONSTS::OPTION_RC_MODE, key[0], edge_ptrs);
    if (rval != MBError::SUCCESS)
        return rval;

    if (edge_ptrs.len_ptr[0] == 0) {
        ReserveData(data.buff, data.data_len, data.data_offset);
        // Add the first edge along this edge
        mm.AddRootEdge(edge_ptrs, key, len, data.data_offset);
        if (data.options & CONSTS::OPTION_RC_MODE) {
            header->rc_count++;
        } else {
            header->count++;
            header->num_update++;
        }
        return MBError::SUCCESS;
    }

    bool inc_count = true;
    int i;
    const uint8_t* key_buff;
    uint8_t tmp_key_buff[NUM_ALPHABET];
    const uint8_t* key_cursor = key;
    int edge_len = edge_ptrs.len_ptr[0];
    int orig_len = len;
    if (edge_len > LOCAL_EDGE_LEN) {
        if (mm.ReadData(tmp_key_buff, edge_len - 1, Get5BInteger(edge_ptrs.ptr)) != edge_len - 1)
            return MBError::READ_ERROR;
        key_buff = tmp_key_buff;
    } else {
        key_buff = edge_ptrs.ptr;
    }
    if (edge_len < len) {
        for (i = 1; i < edge_len; i++) {
            if (key_buff[i - 1] != key[i])
                break;
        }
        if (i >= edge_len) {
            int match_len;
            bool next;
            key_cursor += edge_len;
            len -= edge_len;

            while ((next = mm.FindNext(key_cursor, len, match_len, edge_ptrs, tmp_key_buff))) {
                if (match_len < edge_ptrs.len_ptr[0])
                    break;

                key_cursor += match_len;
                len -= match_len;
                if (len <= 0)
                    break;
            }
            if (!next) {
                ReserveData(data.buff, data.data_len, data.data_offset);
                rval = mm.UpdateNode(edge_ptrs, key_cursor, len, data.data_offset);
            } else if (match_len < static_cast<int>(edge_ptrs.len_ptr[0])) {
                if (len > match_len) {
                    ReserveData(data.buff, data.data_len, data.data_offset);
                    rval = mm.AddLink(edge_ptrs, match_len, key_cursor + match_len, len - match_len,
                        data.data_offset, data);
                } else if (len == match_len) {
                    ReserveData(data.buff, data.data_len, data.data_offset);
                    rval = mm.InsertNode(edge_ptrs, match_len, data.data_offset, data);
                }
            } else if (len == 0) {
                rval = UpdateDataBuffer(edge_ptrs, overwrite, data, inc_count);
            }
        } else {
            ReserveData(data.buff, data.data_len, data.data_offset);
            rval = mm.AddLink(edge_ptrs, i, key_cursor + i, len - i, data.data_offset, data);
        }
    } else {
        for (i = 1; i < len; i++) {
            if (key_buff[i - 1] != key[i])
                break;
        }
        if (i < len) {
            ReserveData(data.buff, data.data_len, data.data_offset);
            rval = mm.AddLink(edge_ptrs, i, key_cursor + i, len - i, data.data_offset, data);
        } else {
            if (edge_ptrs.len_ptr[0] > len) {
                ReserveData(data.buff, data.data_len, data.data_offset);
                rval = mm.InsertNode(edge_ptrs, i, data.data_offset, data);
            } else {
                rval = UpdateDataBuffer(edge_ptrs, overwrite, data, inc_count);
            }
        }
    }

    if (data.options & CONSTS::OPTION_RC_MODE) {
        if (rval == MBError::SUCCESS)
            header->rc_count++;
    } else {
        if (rval == MBError::SUCCESS)
            header->num_update++;
        if (inc_count)
            header->count++;
    }
    // After a successful add, seed prefix cache at canonical 2/3-byte boundaries.
    if (rval == MBError::SUCCESS && prefix_cache) {
        SeedCanonicalBoundariesAfterAdd(key, orig_len, /*from_add=*/true);
    }
    return rval;
}

void Dict::SeedCanonicalBoundariesAfterAdd(const uint8_t* key, int len, bool from_add) const
{
    if (!prefix_cache)
        return;
    if (len < 2)
        return;

    EdgePtrs edge_ptrs;
    int rval = mm.GetRootEdge(0, key[0], edge_ptrs);
    if (rval != MBError::SUCCESS)
        return;
    if (edge_ptrs.len_ptr[0] == 0)
        return;

    const uint8_t* key_cursor = key;
    int remain = len;
    int consumed = 0;
    uint8_t tmp_key_buff[NUM_ALPHABET];

    // Step 1: handle root edge like findInternal
    int edge_len = edge_ptrs.len_ptr[0];
    int edge_len_m1 = edge_len - 1;
    const uint8_t* key_buff;
    // load edge tail
    if (edge_len_m1 > 0) {
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            size_t off = Get5BInteger(edge_ptrs.ptr);
            if (mm.ReadData(tmp_key_buff, edge_len_m1, off) != edge_len_m1)
                return;
            key_buff = tmp_key_buff;
        } else {
            key_buff = edge_ptrs.ptr;
        }
    } else {
        key_buff = nullptr;
    }

    if (edge_len < remain) {
        if (edge_len_m1 <= 0 || memcmp(key_buff, key_cursor + 1, edge_len_m1) == 0) {
            // If 4-byte boundary falls inside this edge and prefix matches, seed a mid-edge entry.
            if (from_add && len >= 4 && consumed < 4 && consumed + edge_len >= 4) {
                uint8_t skip = static_cast<uint8_t>(4 - consumed);
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = skip;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 4, e);
            }
            key_cursor += edge_len;
            consumed += edge_len;
            remain -= edge_len;
            if (remain <= 0) {
                // final match at root: seed at consumed depth
                if (consumed == 4) {
                    PrefixCacheEntry e {};
                    e.edge_offset = edge_ptrs.offset;
                    e.edge_skip = 0;
                    e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                    memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                    prefix_cache->PutAtDepth(key, 4, e);
                } else if (consumed == 3) {
                    PrefixCacheEntry e {};
                    e.edge_offset = edge_ptrs.offset;
                    e.edge_skip = 0;
                    e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                    memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                    prefix_cache->PutAtDepth(key, 3, e);
                } else if (consumed == 2) {
                    PrefixCacheEntry e2 {};
                    e2.edge_offset = edge_ptrs.offset;
                    e2.edge_skip = 0;
                    e2.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                    memcpy(e2.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                    prefix_cache->PutAtDepth(key, 2, e2);
                }
                return;
            }
            if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)
                return; // leaf
            // Defer seeding at this consumed depth until we've loaded the
            // next edge (so cache stores the correct traversal state).
        } else {
            return;
        }
    } else if (edge_len == remain) {
        if (edge_len_m1 <= 0 || memcmp(key_buff, key_cursor + 1, edge_len_m1) == 0) {
            // mirror find: seed at final depth (consumed + edge_len)
            int c = consumed + edge_len;
            if (c == 4) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 4, e);
            } else if (c == 3) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 3, e);
            } else if (c == 2) {
                PrefixCacheEntry e2 {};
                e2.edge_offset = edge_ptrs.offset;
                e2.edge_skip = 0;
                e2.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e2.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 2, e2);
            }
        }
        return;
    } else {
        return; // key shorter than edge
    }

    // Step 2: traverse like traverseFromEdge()
    MBData seed_mbd; // local scratch; options=0 (no special flags)
    seed_mbd.options = 0;
    while (true) {
        rval = mm.NextEdge(key_cursor, edge_ptrs, seed_mbd.node_buff, seed_mbd);
        if (rval != MBError::SUCCESS)
            break;
        edge_len = edge_ptrs.len_ptr[0];
        edge_len_m1 = edge_len - 1;
        // load edge key tail
        if (edge_len_m1 > 0) {
            if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
                size_t off = Get5BInteger(edge_ptrs.ptr);
                if (mm.ReadData(tmp_key_buff, edge_len_m1, off) != edge_len_m1)
                    break;
                key_buff = tmp_key_buff;
            } else {
                key_buff = edge_ptrs.ptr;
            }
        } else {
            key_buff = nullptr;
        }
        // compare tail
        if (edge_len_m1 > 0 && memcmp(key_buff, key_cursor + 1, edge_len_m1) != 0) {
            break;
        }
        // If 4-byte boundary falls inside this edge and prefix matches, seed mid-edge entry.
        if (from_add && len >= 4 && consumed < 4 && consumed + edge_len >= 4) {
            uint8_t skip = static_cast<uint8_t>(4 - consumed);
            PrefixCacheEntry e {};
            e.edge_offset = edge_ptrs.offset;
            e.edge_skip = skip;
            e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
            memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
            prefix_cache->PutAtDepth(key, 4, e);
        }
        // advance
        remain -= edge_len;
        if (remain <= 0) {
            int c = consumed + edge_len;
            if (c == 4) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 4, e);
            } else if (c == 3) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 3, e);
            } else if (c == 2) {
                PrefixCacheEntry e2 {};
                e2.edge_offset = edge_ptrs.offset;
                e2.edge_skip = 0;
                e2.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e2.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 2, e2);
            }
            break;
        }
        if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
            int c = consumed + edge_len;
            if (c == 4) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 4, e);
            } else if (c == 3) {
                PrefixCacheEntry e {};
                e.edge_offset = edge_ptrs.offset;
                e.edge_skip = 0;
                e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 3, e);
            } else if (c == 2) {
                PrefixCacheEntry e2 {};
                e2.edge_offset = edge_ptrs.offset;
                e2.edge_skip = 0;
                e2.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
                memcpy(e2.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
                prefix_cache->PutAtDepth(key, 2, e2);
            }
            break;
        }
        key_cursor += edge_len;
        consumed += edge_len;
        if (consumed == 4) {
            PrefixCacheEntry e {};
            e.edge_offset = edge_ptrs.offset;
            e.edge_skip = 0;
            e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
            memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
            prefix_cache->PutAtDepth(key, 4, e);
        } else if (consumed == 3) {
            PrefixCacheEntry e {};
            e.edge_offset = edge_ptrs.offset;
            e.edge_skip = 0;
            e.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
            memcpy(e.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
            prefix_cache->PutAtDepth(key, 3, e);
        } else if (consumed == 2) {
            PrefixCacheEntry e2 {};
            e2.edge_offset = edge_ptrs.offset;
            e2.edge_skip = 0;
            e2.lf_counter = static_cast<uint32_t>(from_add ? 1 : 2);
            memcpy(e2.edge_buff, edge_ptrs.edge_buff, EDGE_SIZE);
            prefix_cache->PutAtDepth(key, 2, e2);
        }
    }
}

int Dict::ReadDataFromEdge(MBData& data, const EdgePtrs& edge_ptrs) const
{
    size_t data_off;
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        data_off = Get6BInteger(edge_ptrs.offset_ptr);
    } else {
        uint8_t node_buff[NODE_EDGE_KEY_FIRST];
        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, Get6BInteger(edge_ptrs.offset_ptr))
            != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;
        if (!(node_buff[0] & FLAG_NODE_MATCH))
            return MBError::NOT_EXIST;
        data_off = Get6BInteger(node_buff + 2);
    }
    data.data_offset = data_off;

    uint16_t data_len[2];
    // Read data length first
    if (ReadData(reinterpret_cast<uint8_t*>(&data_len[0]), DATA_HDR_BYTE, data_off)
        != DATA_HDR_BYTE)
        return MBError::READ_ERROR;
    data_off += DATA_HDR_BYTE;
    if (data.buff_len < data_len[0] + 1) {
        if (data.Resize(data_len[0]) != MBError::SUCCESS)
            return MBError::NO_MEMORY;
    }

    if (ReadData(data.buff, data_len[0], data_off) != data_len[0])
        return MBError::READ_ERROR;

    data.data_len = data_len[0];
    data.bucket_index = data_len[1];
    return MBError::SUCCESS;
}

// Delete operations:
//   If this is a leaf node, need to remove the edge. Otherwise, unset the match flag.
//   Also need to set the delete flag in the data block so that it can be reclaimed later.
int Dict::DeleteDataFromEdge(MBData& data, EdgePtrs& edge_ptrs)
{
    int rval = MBError::SUCCESS;
    size_t data_off;
    uint16_t data_len;
    int rel_size;

    // Check if this is a leaf node first by using the EDGE_FLAG_DATA_OFF bit
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        data_off = Get6BInteger(edge_ptrs.offset_ptr);
        if (ReadData(reinterpret_cast<uint8_t*>(&data_len), DATA_SIZE_BYTE, data_off)
            != DATA_SIZE_BYTE)
            return MBError::READ_ERROR;
        if (options & CONSTS::OPTION_JEMALLOC) {
            rel_size = data_len + DATA_HDR_BYTE;
        } else {
            rel_size = free_lists->GetAlignmentSize(data_len + DATA_HDR_BYTE);
        }
        ReleaseBuffer(data_off, rel_size);
        rval = mm.RemoveEdgeByIndex(edge_ptrs, data);
    } else {
        // No exception handling in this case
        header->excep_lf_offset = 0;
        header->excep_offset = 0;

        uint8_t node_buff[NODE_EDGE_KEY_FIRST];
        size_t node_off = Get6BInteger(edge_ptrs.offset_ptr);

        // Read node header
        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;

        if (node_buff[0] & FLAG_NODE_MATCH) {
            // Unset the match flag
            node_buff[0] &= ~FLAG_NODE_MATCH;
            mm.WriteData(&node_buff[0], 1, node_off);

            // Release data buffer
            data_off = Get6BInteger(node_buff + 2);
            if (ReadData(reinterpret_cast<uint8_t*>(&data_len), DATA_SIZE_BYTE, data_off)
                != DATA_SIZE_BYTE)
                return MBError::READ_ERROR;

            if (options & CONSTS::OPTION_JEMALLOC) {
                rel_size = data_len + DATA_HDR_BYTE;
            } else {
                rel_size = free_lists->GetAlignmentSize(data_len + DATA_HDR_BYTE);
            }
            ReleaseBuffer(data_off, rel_size);
        } else {
            rval = MBError::NOT_EXIST;
        }
    }

    return rval;
}

int Dict::ReadDataFromNode(MBData& data, const uint8_t* node_ptr) const
{
    size_t data_off = Get6BInteger(node_ptr + 2);
    if (data_off == 0)
        return MBError::NOT_EXIST;

    data.data_offset = data_off;

    // Read data length first
    uint16_t data_len[2];
    if (ReadData(reinterpret_cast<uint8_t*>(&data_len[0]), DATA_HDR_BYTE, data_off)
        != DATA_HDR_BYTE)
        return MBError::READ_ERROR;
    data_off += DATA_HDR_BYTE;

    if (data.buff_len < data_len[0] + 1) {
        if (data.Resize(data_len[0]) != MBError::SUCCESS)
            return MBError::NO_MEMORY;
    }
    if (ReadData(data.buff, data_len[0], data_off) != data_len[0])
        return MBError::READ_ERROR;

    data.data_len = data_len[0];
    data.bucket_index = data_len[1];
    return MBError::SUCCESS;
}

void Dict::PrintStats(std::ostream* out_stream) const
{
    if (out_stream != NULL)
        return PrintStats(*out_stream);
    return PrintStats(std::cout);
}

void Dict::PrintStats(std::ostream& out_stream) const
{
    if (status != MBError::SUCCESS)
        return;

    out_stream << "DB stats:\n";
    out_stream << "\tWriter option: " << header->writer_options << std::endl;
    out_stream << "\tNumber of DB writer: " << header->num_writer << std::endl;
    out_stream << "\tNumber of DB reader: " << header->num_reader << std::endl;
    out_stream << "\tEntry count in DB: " << header->count << std::endl;
    out_stream << "\tEntry count per bucket: " << header->entry_per_bucket << std::endl;
    out_stream << "\tEviction bucket index: " << header->eviction_bucket_index << std::endl;
    out_stream << "\tData block size: " << header->data_block_size << std::endl;
    // The pending_data_buff_size in jemalloc mode is the total size of all allocated data buffers
    // The pending_data_buff_size in non-jemalloc mode is the total size of all free data buffers
    if (options & CONSTS::OPTION_JEMALLOC) {
        out_stream << "\tAllocated data memory size: " << header->pending_data_buff_size << std::endl;
    } else if (free_lists != nullptr) {
        out_stream << "\tData size: " << header->m_data_offset << std::endl;
        out_stream << "\tPending buffer size: " << header->pending_data_buff_size << std::endl;
        out_stream << "\tTrackable buffer size: " << free_lists->GetTotSize() << std::endl;
    }
    mm.PrintStats(out_stream);

    kv_file->PrintStats(out_stream);
    qmgr.PrintStats(out_stream, header);

#ifdef __DEBUG__
    out_stream << "Size of tracking buffer: " << buffer_map.size() << std::endl;
#endif
}

int64_t Dict::Count() const
{
    if (header == NULL) {
        Logger::Log(LOG_LEVEL_WARN, "db was not initialized successfully: %s",
            MBError::get_error_str(status));
        return 0;
    }

    return header->count;
}

// For DB iterator
int Dict::ReadNextEdge(const uint8_t* node_buff, EdgePtrs& edge_ptrs,
    int& match, MBData& data, std::string& match_str,
    size_t& node_off, bool rd_kv) const
{
    if (edge_ptrs.curr_nt > static_cast<int>(node_buff[1]))
        return MBError::OUT_OF_BOUND;

    if (mm.ReadData(edge_ptrs.edge_buff, EDGE_SIZE, edge_ptrs.offset) != EDGE_SIZE)
        return MBError::READ_ERROR;

    node_off = 0;
    match_str = "";

    int rval = MBError::SUCCESS;
    InitTempEdgePtrs(edge_ptrs);
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        // match of leaf node
        match = MATCH_EDGE;
        if (rd_kv) {
            rval = ReadDataFromEdge(data, edge_ptrs);
            if (rval != MBError::SUCCESS)
                return rval;
        }
    } else {
        match = MATCH_NONE;
        if (edge_ptrs.len_ptr[0] > 0) {
            node_off = Get6BInteger(edge_ptrs.offset_ptr);
            if (rd_kv)
                rval = ReadNodeMatch(node_off, match, data);
        }
    }

    if (edge_ptrs.len_ptr[0] > 0 && rd_kv) {
        int edge_len_m1 = edge_ptrs.len_ptr[0] - 1;
        match_str = std::string(1, (const char)node_buff[NODE_EDGE_KEY_FIRST + edge_ptrs.curr_nt]);
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            if (mm.ReadData(data.node_buff, edge_len_m1, Get5BInteger(edge_ptrs.ptr)) != edge_len_m1)
                return MBError::READ_ERROR;
            match_str += std::string(reinterpret_cast<char*>(data.node_buff), edge_len_m1);
        } else if (edge_len_m1 > 0) {
            match_str += std::string(reinterpret_cast<char*>(edge_ptrs.ptr), edge_len_m1);
        }
    }

    edge_ptrs.curr_nt++;
    edge_ptrs.offset += EDGE_SIZE;
    return rval;
}

// For DB iterator
int Dict::ReadNode(size_t node_off, uint8_t* node_buff, EdgePtrs& edge_ptrs,
    int& match, MBData& data, bool rd_kv) const
{
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        return MBError::READ_ERROR;

    edge_ptrs.curr_nt = 0;
    int nt = node_buff[1] + 1;
    node_off += NODE_EDGE_KEY_FIRST;
    if (mm.ReadData(node_buff + NODE_EDGE_KEY_FIRST, nt, node_off) != nt)
        return MBError::READ_ERROR;

    int rval = MBError::SUCCESS;
    edge_ptrs.offset = node_off + nt;
    if (node_buff[0] & FLAG_NODE_MATCH) {
        // match of non-leaf node
        match = MATCH_NODE;
        if (rd_kv)
            rval = ReadDataFromNode(data, node_buff);
    } else {
        // no match at the non-leaf node
        match = MATCH_NONE;
    }

    return rval;
}

void Dict::ReadNodeHeader(size_t node_off, int& node_size, int& match,
    size_t& data_offset, size_t& data_link_offset)
{
    uint8_t node_buff[NODE_EDGE_KEY_FIRST];
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        throw (int)MBError::READ_ERROR;

    node_size = mm.GetNodeSizePtr()[node_buff[1]];
    if (node_buff[0] & FLAG_NODE_MATCH) {
        match = MATCH_NODE;
        data_offset = Get6BInteger(node_buff + 2);
        data_link_offset = node_off + 2;
    }
}

int Dict::ReadNodeMatch(size_t node_off, int& match, MBData& data) const
{
    uint8_t node_buff[NODE_EDGE_KEY_FIRST];
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        return MBError::READ_ERROR;

    int rval = MBError::SUCCESS;
    if (node_buff[0] & FLAG_NODE_MATCH) {
        match = MATCH_NODE;
        rval = ReadDataFromNode(data, node_buff);
        if (rval != MBError::SUCCESS)
            return rval;
    }

    return MBError::SUCCESS;
}

size_t Dict::GetRootOffset() const
{
    return mm.GetRootOffset();
}

// For DB iterator
int Dict::ReadRootNode(uint8_t* node_buff, EdgePtrs& edge_ptrs, int& match,
    MBData& data) const
{
    size_t root_off;
    if (data.options & CONSTS::OPTION_RC_MODE)
        root_off = header->rc_root_offset;
    else
        root_off = mm.GetRootOffset();
    return ReadNode(root_off, node_buff, edge_ptrs, match, data);
}

int Dict::Remove(const uint8_t* key, int len)
{
    MBData data(0, CONSTS::OPTION_FIND_AND_STORE_PARENT);
    return Remove(key, len, data);
}

int Dict::Remove(const uint8_t* key, int len, MBData& data)
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        return MBError::NOT_ALLOWED;
    }
    if (data.options & CONSTS::OPTION_RC_MODE) {
        // FIXME Remove in RC mode not implemented yet!!!
        return MBError::INVALID_ARG;
    }

    // The DELETE flag must be set
    if (!(data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT))
        return MBError::INVALID_ARG;

    int rval;
    {
        detail::SearchEngine engine(*this);
        rval = engine.find(key, len, data);
    }
    if (rval == MBError::IN_DICT) {
        rval = DeleteDataFromEdge(data, data.edge_ptrs);
        while (rval == MBError::TRY_AGAIN) {
            data.Clear();
            len -= data.edge_ptrs.len_ptr[0];
#ifdef __DEBUG__
            assert(len > 0);
#endif
            {
                detail::SearchEngine engine(*this);
                rval = engine.find(key, len, data);
            }
            if (MBError::IN_DICT == rval) {
                rval = mm.RemoveEdgeByIndex(data.edge_ptrs, data);
            }
        }
    }

    if (rval == MBError::SUCCESS) {
        header->count--;
    }

    return rval;
}

int Dict::RemoveAll()
{
    int rval = MBError::SUCCESS;

    mm.ClearMem(); // clear memory will re-initialize jemalloc
    if (options & CONSTS::OPTION_JEMALLOC) {
        mm.InitRootNode();
        kv_file->ResetJemalloc();
        for (int c = 0; c < NUM_ALPHABET; c++) {
            rval = mm.ClearRootEdge(c);
            if (rval != MBError::SUCCESS)
                break;
        }
    } else {
        for (int c = 0; c < NUM_ALPHABET; c++) {
            rval = mm.ClearRootEdge(c);
            if (rval != MBError::SUCCESS)
                break;
        }
        header->m_data_offset = GetStartDataOffset();
        free_lists->Empty();
    }
    header->pending_data_buff_size = 0;
    header->count = 0;
    header->eviction_bucket_index = 0;
    header->num_update = 0;
    return rval;
}

pthread_mutex_t* Dict::GetShmLockPtr() const
{
    return &(slaq->lock);
}

AsyncNode* Dict::GetAsyncQueuePtr() const
{
    return slaq->queue;
}

// Reserve buffer and write to it
// The pending_data_buff_size in jemalloc mode is the total size of all allocated data buffers
// The pending_data_buff_size in non-jemalloc mode is the total size of all free data buffers
void Dict::ReserveData(const uint8_t* buff, int size, size_t& offset)
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        int buf_size = size + DATA_HDR_BYTE;
        void* ptr = kv_file->Malloc(buf_size, offset);
        if (ptr == NULL) {
            Logger::Log(LOG_LEVEL_ERROR, "failed to allocate memory for data buffer");
            throw MBError::NO_MEMORY;
        }
        uint16_t dsize[2];
        dsize[0] = static_cast<uint16_t>(size);
        // store bucket index for LRU eviction
        dsize[1] = (header->num_update / header->entry_per_bucket) % 0xFFFF;
        if (dsize[1] == header->eviction_bucket_index && header->num_update > header->entry_per_bucket) {
            header->eviction_bucket_index++;
        }
        memcpy(ptr, &dsize[0], DATA_HDR_BYTE);
        memcpy(static_cast<uint8_t*>(ptr) + DATA_HDR_BYTE, buff, size);
        // update the size of pending data buffer in the header
        header->pending_data_buff_size += (buf_size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
    } else {
        reserveDataFL(buff, size, offset);
    }

#ifdef __DEBUG__
    // offset is allocated, add it to the tracking map
    // note if reserve failed, an exception will be thrown before this point
    add_tracking_buffer(offset, size);
#endif
}

void Dict::reserveDataFL(const uint8_t* buff, int size, size_t& offset)
{
#ifdef __DEBUG__
    assert(size <= CONSTS::MAX_DATA_SIZE);
#endif
    int buf_size = free_lists->GetAlignmentSize(size + DATA_HDR_BYTE);
    int buf_index = free_lists->GetBufferIndex(buf_size);
    uint16_t dsize[2];
    dsize[0] = static_cast<uint16_t>(size);
    // store bucket index for LRU eviction
    dsize[1] = (header->num_update / header->entry_per_bucket) % 0xFFFF;
    if (dsize[1] == header->eviction_bucket_index && header->num_update > header->entry_per_bucket) {
        header->eviction_bucket_index++;
    }

    if (free_lists->GetBufferCountByIndex(buf_index) > 0) {
        offset = free_lists->RemoveBufferByIndex(buf_index);
        WriteData(reinterpret_cast<const uint8_t*>(&dsize[0]), DATA_HDR_BYTE, offset);
        WriteData(buff, size, offset + DATA_HDR_BYTE);
        header->pending_data_buff_size -= buf_size;
    } else {
        size_t old_off = header->m_data_offset;
        uint8_t* ptr;

        int rval = kv_file->Reserve(header->m_data_offset, buf_size, ptr);
        if (rval != MBError::SUCCESS)
            throw rval;

        // Checking missing buffer due to alignment
        if (old_off < header->m_data_offset) {
            ReleaseAlignmentBuffer(old_off, header->m_data_offset);
            header->pending_data_buff_size += header->m_data_offset - old_off;
        }

        offset = header->m_data_offset;
        header->m_data_offset += buf_size;
        if (ptr != NULL) {
            memcpy(ptr, &dsize[0], DATA_HDR_BYTE);
            memcpy(ptr + DATA_HDR_BYTE, buff, size);
        } else {
            WriteData(reinterpret_cast<const uint8_t*>(&dsize[0]), DATA_HDR_BYTE, offset);
            WriteData(buff, size, offset + DATA_HDR_BYTE);
        }
    }
}

int Dict::ReleaseBuffer(size_t offset, int size)
{
#ifdef __DEBUG__
    remove_tracking_buffer(offset, size);
#endif
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->Free(offset);
        size_t rel_size = ((size_t)size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
        header->pending_data_buff_size -= (int64_t)rel_size;
        if (header->pending_data_buff_size < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "pending data buffer size is negative: %d",
                header->pending_data_buff_size);
            header->pending_data_buff_size = 0;
        }
        return MBError::SUCCESS;
    } else {
        header->pending_data_buff_size += size;
        return free_lists->ReleaseBuffer(offset, size);
    }
}

void Dict::ReleaseAlignmentBuffer(size_t offset, size_t alignment_off)
{
    // alignment is not a real buffer, so no need to track it
    if (options & CONSTS::OPTION_JEMALLOC) {
        // no-op
    } else {
        free_lists->ReleaseAlignmentBuffer(offset, alignment_off);
    }
}

int Dict::ReleaseBuffer(size_t offset)
{
#ifdef __DEBUG__
    remove_tracking_buffer(offset);
#endif
    // First read the size of the data buffer
    uint16_t data_size;
    if (ReadData(reinterpret_cast<uint8_t*>(&data_size), DATA_SIZE_BYTE, offset) != DATA_SIZE_BYTE) {
        if (options & CONSTS::OPTION_JEMALLOC) {
            // For jemalloc mode, we can just free the buffer and return
            kv_file->Free(offset);
        }
        return MBError::READ_ERROR;
    }
    data_size += DATA_HDR_BYTE;
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->Free(offset);

        size_t rel_size = ((size_t)data_size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
        header->pending_data_buff_size -= (int64_t)rel_size;
        if (header->pending_data_buff_size < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "pending data buffer size is negative: %d",
                header->pending_data_buff_size);
            header->pending_data_buff_size = 0;
        }
        return MBError::SUCCESS;
    } else {
        int rel_size = free_lists->GetAlignmentSize(data_size);
        header->pending_data_buff_size += rel_size;
        return free_lists->ReleaseBuffer(offset, rel_size);
    }
}

int Dict::UpdateDataBuffer(EdgePtrs& edge_ptrs, bool overwrite, MBData& mbd, bool& inc_count)
{
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        inc_count = false;
        // leaf node
        mbd.data_offset = Get6BInteger(edge_ptrs.offset_ptr);
        if (!overwrite)
            return MBError::IN_DICT;
        if (ReleaseBuffer(mbd.data_offset) != MBError::SUCCESS)
            Logger::Log(LOG_LEVEL_WARN, "failed to release data buffer: %llu", mbd.data_offset);
        ReserveData(mbd.buff, mbd.data_len, mbd.data_offset);
        Write6BInteger(edge_ptrs.offset_ptr, mbd.data_offset);

        memcpy(header->excep_buff, edge_ptrs.offset_ptr, OFFSET_SIZE);
#ifdef __LOCK_FREE__
        header->excep_lf_offset = edge_ptrs.offset;
        lfree.WriterLockFreeStart(edge_ptrs.offset);
#endif
        header->excep_updating_status = EXCEP_STATUS_ADD_DATA_OFF;
        mm.WriteData(edge_ptrs.offset_ptr, OFFSET_SIZE, edge_ptrs.offset + EDGE_NODE_LEADING_POS);
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStop();
#endif
        header->excep_updating_status = EXCEP_STATUS_NONE;
    } else {
        uint8_t* node_buff = header->excep_buff;
        size_t node_off = Get6BInteger(edge_ptrs.offset_ptr);

        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;

        if (node_buff[0] & FLAG_NODE_MATCH) {
            inc_count = false;
            mbd.data_offset = Get6BInteger(node_buff + 2);
            if (!overwrite)
                return MBError::IN_DICT;
            if (ReleaseBuffer(mbd.data_offset) != MBError::SUCCESS)
                Logger::Log(LOG_LEVEL_WARN, "failed to release data buffer %llu", mbd.data_offset);
            node_buff[NODE_EDGE_KEY_FIRST] = 0;
        } else {
            // set the match flag
            node_buff[0] |= FLAG_NODE_MATCH;

            node_buff[NODE_EDGE_KEY_FIRST] = 1;
        }

        ReserveData(mbd.buff, mbd.data_len, mbd.data_offset);
        Write6BInteger(node_buff + 2, mbd.data_offset);

        header->excep_offset = node_off;
#ifdef __LOCK_FREE__
        header->excep_lf_offset = edge_ptrs.offset;
        lfree.WriterLockFreeStart(edge_ptrs.offset);
#endif
        header->excep_updating_status = EXCEP_STATUS_ADD_NODE;
        mm.WriteData(node_buff, NODE_EDGE_KEY_FIRST, node_off);
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStop();
#endif
        header->excep_updating_status = EXCEP_STATUS_NONE;
    }

    return MBError::SUCCESS;
}

// delta should be either +1 or -1.
void Dict::UpdateNumReader(int delta) const
{
    header->num_reader += delta;
    if (header->num_reader < 0)
        header->num_reader = 0;

    Logger::Log(LOG_LEVEL_DEBUG, "number of reader is set to: %d",
        header->num_reader);
}

// delta should be either +1 or -1.
int Dict::UpdateNumWriter(int delta) const
{
    if (delta > 0) {
        // Only one writer allowed
        if (header->num_writer > 0) {
            Logger::Log(LOG_LEVEL_WARN, "writer was not shutdown cleanly previously");
            header->num_writer = 1;
            // Reset number of reader too.
            header->num_reader = 0;
            return MBError::WRITER_EXIST;
        }

        header->num_writer = 1;
    } else if (delta < 0) {
        header->num_writer = 0;
        header->lock_free.offset = MAX_6B_OFFSET;
    }

    Logger::Log(LOG_LEVEL_DEBUG, "number of writer is set to: %d", header->num_writer);
    return MBError::SUCCESS;
}

DictMem* Dict::GetMM() const
{
    return (DictMem*)&mm;
}

size_t Dict::GetStartDataOffset() const
{
    // Start of user data within _mabain_d: after embedded prefix cache if present.
    if (header && header->pfxcache_size > 0) {
        return header->pfxcache_offset + header->pfxcache_size;
    }
    return DATA_HEADER_SIZE;
}

LockFree* Dict::GetLockFreePtr()
{
    return &lfree;
}

void Dict::Flush() const
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER))
        return;

    if (kv_file != NULL)
        kv_file->Flush();
    mm.Flush();
}

void Dict::Purge() const
{
    if ((options & CONSTS::ACCESS_MODE_WRITER) && (options & CONSTS::OPTION_JEMALLOC)) {
        if (kv_file != nullptr)
            kv_file->Purge();
        mm.Purge();
    }
}

// Cache is enabled implicitly based on DB layout; no public toggles.

/* MaybePutCache removed: inlined seeding logic inside SeedCanonicalBoundariesAfterAdd */

void Dict::GetPrefixCacheStats(uint64_t& hit, uint64_t& miss, uint64_t& put,
    size_t& entries, int& n) const
{
    if (!prefix_cache) {
        hit = miss = put = 0;
        entries = 0;
        n = 0;
        return;
    }
    hit = 0;
    miss = 0;
    put = prefix_cache->PutCount();
    entries = prefix_cache->Size();
    n = prefix_cache->PrefixLen();
}

void Dict::ResetPrefixCacheStats()
{
    if (prefix_cache)
        prefix_cache->ResetStats();
}

void Dict::PrintPrefixCacheStats(std::ostream& os) const
{
    uint64_t hit, miss, put;
    size_t entries;
    int pn;
    GetPrefixCacheStats(hit, miss, put, entries, pn);
    if (prefix_cache) {
        size_t e2 = prefix_cache->Size2();
        size_t e3 = prefix_cache->Size3();
        size_t e4 = prefix_cache->Size4();
        size_t m2 = prefix_cache->Memory2();
        size_t m3 = prefix_cache->Memory3();
        size_t m4 = prefix_cache->Memory4();
        os << "PrefixCache: enabled=1"
           << " entries2=" << e2
           << " entries3=" << e3
           << " entries4=" << e4
           << " mem2=" << m2
           << " mem3=" << m3
           << " mem4=" << m4
           << " entries_total=" << entries
           << " put=" << put
           << std::endl;
    } else {
        os << "PrefixCache: enabled=0"
           << " entries_total=0"
           << " put=0" << std::endl;
    }
}

PrefixCache* Dict::ActivePrefixCache() const
{
    if (prefix_cache)
        return prefix_cache.get();
    return nullptr;
}

// Recovery from abnormal writer terminations (segfault, kill -9 etc)
// during DB updates (insertion, replacing and deletion).
int Dict::ExceptionRecovery()
{
    if (header == NULL)
        return MBError::NOT_INITIALIZED;

    int rval = MBError::SUCCESS;
    if (header->excep_updating_status == EXCEP_STATUS_NONE) {
        Logger::Log(LOG_LEVEL_DEBUG, "writer was shutdown successfully previously");
        return rval;
    }

    Logger::Log(LOG_LEVEL_INFO, "writer was not shutdown gracefully with exception status %d",
        header->excep_updating_status);
    // Dumper header before running recover
    std::ofstream* logstream = Logger::GetLogStream();
    if (logstream != NULL) {
        PrintHeader(*logstream);
    } else {
        PrintHeader(std::cout);
    }

    switch (header->excep_updating_status) {
    case EXCEP_STATUS_ADD_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, EDGE_SIZE, header->excep_lf_offset);
        header->count++;
        break;
    case EXCEP_STATUS_ADD_DATA_OFF:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE,
            header->excep_lf_offset + EDGE_NODE_LEADING_POS);
        break;
    case EXCEP_STATUS_ADD_NODE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, NODE_EDGE_KEY_FIRST,
            header->excep_offset);
        if (header->excep_buff[NODE_EDGE_KEY_FIRST])
            header->count++;
        break;
    case EXCEP_STATUS_REMOVE_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        Write6BInteger(header->excep_buff, header->excep_offset);
        mm.WriteData(header->excep_buff, OFFSET_SIZE,
            header->excep_lf_offset + EDGE_NODE_LEADING_POS);
        break;
    case EXCEP_STATUS_CLEAR_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(DictMem::empty_edge, EDGE_SIZE, header->excep_lf_offset);
        header->count--;
        break;
    case EXCEP_STATUS_RC_NODE:
    case EXCEP_STATUS_RC_DATA:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE, header->excep_offset);
        break;
    case EXCEP_STATUS_RC_EDGE_STR:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE - 1, header->excep_offset);
        break;
    default:
        Logger::Log(LOG_LEVEL_ERROR, "unknown exception status: %d",
            header->excep_updating_status);
        rval = MBError::INVALID_ARG;
    }
#ifdef __LOCK_FREE__
    lfree.WriterLockFreeStop();
#endif

    if (rval == MBError::SUCCESS) {
        header->excep_updating_status = EXCEP_STATUS_NONE;
        Logger::Log(LOG_LEVEL_INFO, "successfully recovered from abnormal termination");
    } else {
        Logger::Log(LOG_LEVEL_ERROR, "failed to recover from abnormal termination");
    }

    return rval;
}

void Dict::WriteData(const uint8_t* buff, unsigned len, size_t offset) const
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->MemWrite(buff, len, offset);
    } else {
        if (offset + len > header->m_data_offset) {
            std::cerr << "invalid dict write: " << offset << " " << len << " "
                      << header->m_data_offset << "\n";
            throw (int)MBError::OUT_OF_BOUND;
        }

        if (kv_file->RandomWrite(buff, len, offset) != len)
            throw (int)MBError::WRITE_ERROR;
    }
}

int Dict::ReadDataByOffset(size_t offset, MBData& data) const
{
    uint16_t hdr[2];
    if (ReadData(reinterpret_cast<uint8_t*>(&hdr[0]), DATA_HDR_BYTE, offset) != DATA_HDR_BYTE) {
        return MBError::READ_ERROR;
    }

    // store data length
    data.data_len = hdr[0];
    // store bucket index
    data.bucket_index = hdr[1];
    // resize data buffer using size from header
    data.Resize(hdr[0]);
    offset += DATA_HDR_BYTE;
    if (ReadData(data.buff, hdr[0], offset) != hdr[0]) {
        return MBError::READ_ERROR;
    }
    return MBError::SUCCESS;
}

// Prefix traversal moved to SearchEngine

}
