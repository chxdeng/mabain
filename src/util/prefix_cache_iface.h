// Unified interface for prefix caches (local and shared)
#pragma once

#include <cstddef>
#include <cstdint>

#include "drm_base.h" // for EDGE_SIZE

namespace mabain {

struct PrefixCacheEntry {
    size_t edge_offset;
    uint8_t edge_buff[EDGE_SIZE];
    // Number of bytes already consumed within the current edge label
    // when this entry is used (1..edge_len). 0 means start-of-edge.
    uint8_t edge_skip;
    uint8_t reserved_[3];
    uint32_t lf_counter;
};

class PrefixCacheIface {
public:
    virtual ~PrefixCacheIface() = default;
    virtual void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) = 0;
    // Write only the specified table depth (2 or 3) without touching the other.
    virtual void PutAtDepth(const uint8_t* key, int depth, const PrefixCacheEntry& in) = 0;
    // Return matched prefix length on hit (e.g., 3 or 2 for non-shared cache), 0 on miss.
    // Default implementation bridges to Get() if overridden only in one side.
    virtual int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const = 0;
    virtual int PrefixLen() const = 0;
    virtual bool IsShared() const = 0;
};

} // namespace mabain
