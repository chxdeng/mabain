// Unified interface for prefix caches (local and shared)
#pragma once

#include <cstddef>
#include <cstdint>

#include "drm_base.h" // for EDGE_SIZE

namespace mabain {

struct PrefixCacheEntry {
    size_t edge_offset;
    uint8_t edge_buff[EDGE_SIZE];
    uint32_t lf_counter;
};

class PrefixCacheIface {
public:
    virtual ~PrefixCacheIface() = default;
    virtual bool Get(const uint8_t* key, int len, PrefixCacheEntry& out) const = 0;
    virtual void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) = 0;
    // Return matched prefix length on hit (e.g., 3 or 2 for non-shared cache), 0 on miss.
    // Default implementation bridges to Get() if overridden only in one side.
    virtual int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const = 0;
    virtual int PrefixLen() const = 0;
    virtual bool IsShared() const = 0;
};

} // namespace mabain
