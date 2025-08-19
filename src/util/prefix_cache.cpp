/**
 * Prefix cache implementation
 */

#include "util/prefix_cache.h"
#include <algorithm>

namespace mabain {

static inline size_t floor_pow2(size_t x) {
    if (x == 0) return 0;
    // Round down to the highest power of two <= x
    size_t p = 1;
    while ((p << 1) && ((p << 1) <= x)) p <<= 1;
    return p;
}

static inline size_t cap3_from_capacity(size_t capacity)
{
    // Allocate 3-byte table only for capacity in excess of 2-byte coverage (65536)
    if (capacity <= 65536) return 0;
    size_t excess = capacity - 65536;
    size_t capped = std::min<size_t>(excess, (1u << 24));
    size_t p2 = floor_pow2(capped);
    return p2; // may be 0 if excess < 1
}

PrefixCache::PrefixCache(size_t capacity)
    : cap2(0)
    , cap3(0)
{
    // Validation: capacity cannot exceed total number of 3-byte prefixes (2^24)
    size_t norm_cap = std::min<size_t>(capacity, (1u << 24));

    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    // Minimum table2 size is 10240 when capacity > 0
    if (norm_cap > 0 && c2 < 10240) c2 = 10240;
    const_cast<size_t&>(cap2) = c2;
    const_cast<size_t&>(cap3) = cap3_from_capacity(norm_cap);

    if (cap2 == 0) {
        // Should not happen because Dict::EnablePrefixCache checks capacity>0
        table2.resize(1);
        filled2.assign(1, 0);
        mask2 = 0;
        use_mask2 = true;
    } else {
        table2.resize(cap2);
        filled2.assign(cap2, 0);
        keys2.assign(cap2, 0);
        // Use bitmask when power-of-two; otherwise use modulo
        use_mask2 = ((cap2 & (cap2 - 1)) == 0);
        mask2 = use_mask2 ? cap2 - 1 : 0;
    }
    if (cap3 > 0) {
        table3.resize(cap3); // 3-byte direct table sized by power-of-two capacity
        filled3.assign(cap3, 0);
        keys3.assign(cap3, 0);
        mask3 = cap3 - 1;
    } else {
        mask3 = 0;
    }
}

inline bool PrefixCache::build2(const uint8_t* key, int len, uint16_t& p2) const
{
    if (key == nullptr || len < 2)
        return false;
    // Big endian: key[0] is most significant
    p2 = static_cast<uint16_t>((static_cast<uint16_t>(key[0]) << 8) |
                               static_cast<uint16_t>(key[1]));
    return true;
}

inline bool PrefixCache::build3(const uint8_t* key, int len, uint32_t& p3) const
{
    if (key == nullptr || len < 3)
        return false;
    p3 = (static_cast<uint32_t>(key[0]) << 16) |
         (static_cast<uint32_t>(key[1]) << 8)  |
         (static_cast<uint32_t>(key[2]));
    return true;
}

bool PrefixCache::Get(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        if (filled3[idx3] && keys3[idx3] == p3) {
            out = table3[idx3];
            ++hit_count;
            return true;
        }
    }
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = use_mask2 ? (static_cast<size_t>(p2) & mask2)
                                : (static_cast<size_t>(p2) % cap2);
        if (filled2[idx2] && keys2[idx2] == p2) {
            out = table2[idx2];
            ++hit_count;
            return true;
        }
    }
    ++miss_count;
    return false;
}

int PrefixCache::GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        if (filled3[idx3] && keys3[idx3] == p3) {
            out = table3[idx3];
            ++hit_count;
            return 3;
        }
    }
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = use_mask2 ? (static_cast<size_t>(p2) & mask2)
                                : (static_cast<size_t>(p2) % cap2);
        if (filled2[idx2] && keys2[idx2] == p2) {
            out = table2[idx2];
            ++hit_count;
            return 2;
        }
    }
    ++miss_count;
    return 0;
}

void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    // Insert into 3-byte table if we have at least 3 bytes
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        if (!filled3[idx3]) {
            filled3[idx3] = 1;
            ++size3;
        }
        keys3[idx3] = p3;
        table3[idx3] = in;
        ++put_count;
    }
    // Also insert into 2-byte table if we have at least 2 bytes
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = use_mask2 ? (static_cast<size_t>(p2) & mask2)
                                : (static_cast<size_t>(p2) % cap2);
        if (!filled2[idx2]) {
            filled2[idx2] = 1;
            if (size2 < cap2) ++size2;
        }
        keys2[idx2] = p2;
        table2[idx2] = in;
        ++put_count;
    }
}

size_t PrefixCache::Size() const
{
    size_t s2 = static_cast<size_t>(std::count(filled2.begin(), filled2.end(), 1));
    return s2 + size3;
}

void PrefixCache::Clear()
{
    std::fill(filled2.begin(), filled2.end(), 0);
    size2 = 0;
    std::fill(filled3.begin(), filled3.end(), 0);
    size3 = 0;
}

} // namespace mabain
