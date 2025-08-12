/**
 * Prefix cache implementation
 */

#include "util/prefix_cache.h"
#include <algorithm>
#include <cctype>

namespace mabain {

PrefixCache::PrefixCache(int prefix_len, size_t capacity)
    : n(prefix_len)
    , cap(capacity)
    , fast4(false) // disable fast4 path for stability; fallback to map path
{
    map.reserve(cap);
    if (fast4) {
        slots4.resize(65536);
        filled4.assign(65536, 0);
    }
}

bool PrefixCache::BuildKey(const uint8_t* key, int len, uint64_t& out) const
{
    if (key == nullptr || len < n || n <= 0 || n > 8)
        return false;
    // Pack first n bytes into low bytes of u64
    uint64_t v = 0;
    for (int i = 0; i < n; i++) {
        v |= static_cast<uint64_t>(key[i]) << (i * 8);
    }
    out = v;
    return true;
}

static inline int hex_val(uint8_t c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

bool PrefixCache::BuildIndex4(const uint8_t* key, int len, uint32_t& idx) const
{
    if (!fast4 || key == nullptr || len < 4)
        return false;
    // Map 4 hex characters (nibbles) into a 16-bit index (0..65535)
    int h0 = hex_val(key[0]);
    int h1 = hex_val(key[1]);
    int h2 = hex_val(key[2]);
    int h3 = hex_val(key[3]);
    if ((h0 | h1 | h2 | h3) < 0)
        return false;
    idx = static_cast<uint32_t>((h0 << 12) | (h1 << 8) | (h2 << 4) | h3);
    return true;
}

bool PrefixCache::Get(const uint8_t* key, int len, PrefixCacheEntry& out)
{
    if (fast4) {
        uint32_t idx;
        if (BuildIndex4(key, len, idx)) {
            if (!filled4[idx]) {
                ++miss_count;
                return false;
            }
            out = slots4[idx];
            ++hit_count;
            return true;
        }
        // fall through to map path if non-hex
    }
    {
        uint64_t skey;
        if (!BuildKey(key, len, skey))
            return false;
        auto it = map.find(skey);
        if (it == map.end()) {
            ++miss_count;
            return false;
        }
        out = it->second;
        ++hit_count;
        return true;
    }
}

void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    if (fast4) {
        uint32_t idx;
        if (BuildIndex4(key, len, idx)) {
            if (!filled4[idx]) {
                filled4[idx] = 1;
                ++put_count;
            }
            slots4[idx] = in;
            return;
        }
        // fall through to map path if non-hex
    }
    {
        uint64_t skey;
        if (!BuildKey(key, len, skey))
            return;
        if (map.size() >= cap) {
            // Simple eviction: erase an arbitrary element (first in bucket)
            auto it = map.begin();
            if (it != map.end())
                map.erase(it);
        }
        map[skey] = in;
        ++put_count;
    }
}

size_t PrefixCache::Size() const
{
    return fast4 ? static_cast<size_t>(std::count(filled4.begin(), filled4.end(), 1))
                 : map.size();
}

void PrefixCache::Clear()
{
    if (fast4) {
        std::fill(filled4.begin(), filled4.end(), 0);
    } else {
        map.clear();
    }
}

} // namespace mabain
