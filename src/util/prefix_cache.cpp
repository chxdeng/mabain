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
    , fast_dec(false)
    , dec_cap(0)
    , dec_size(0)
    , fast4(false) // disable fast4 path for stability; fallback to map path
{
    map.reserve(cap);
    // Enable fast decimal path for integer-string keys using open addressing.
    if (n > 0) {
        fast_dec = true;
        // Choose table capacity as power-of-two >= 2*cap to keep load factor <= 0.5
        size_t desired = cap > 0 ? cap * 2 : 65536;
        size_t p2 = 1;
        while (p2 < desired) p2 <<= 1;
        dec_cap = p2;
        dec_size = 0;
        dec_keys.assign(dec_cap, 0);
        dec_lens.assign(dec_cap, 0);
        dec_state.assign(dec_cap, 0);
        dec_vals.resize(dec_cap);
    }
    if (fast4) {
        slots4.resize(65536);
        filled4.assign(65536, 0);
    }
}

bool PrefixCache::BuildKey(const uint8_t* key, int len, uint64_t& out) const
{
    // Allow caching for any prefix length m where m <= n.
    // If the key length is less than n, use the full key length as the prefix;
    // otherwise use the configured prefix length n.
    if (key == nullptr || len <= 0 || n <= 0 || n > 8)
        return false;
    const int use_len = std::min(len, n);
    // Pack first use_len bytes into low bytes of u64
    uint64_t v = 0;
    for (int i = 0; i < use_len; i++) {
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

bool PrefixCache::BuildIndexDec(const uint8_t* key, int len, int& used_len, uint32_t& idx) const
{
    if (!fast_dec || key == nullptr || len <= 0)
        return false;
    used_len = std::min(len, n);
    // compute decimal value of first used_len digits; bail if non-digit
    uint64_t v = 0;
    for (int i = 0; i < used_len; ++i) {
        uint8_t c = key[i];
        if (c < '0' || c > '9')
            return false; // not a pure decimal string; fallback
        v = v * 10 + static_cast<uint64_t>(c - '0');
    }
    // For n <= 6, v fits in 32-bit
    idx = static_cast<uint32_t>(v);
    return true;
}

inline uint64_t PrefixCache::HashDecKey(uint32_t idx, uint8_t used_len) const
{
    uint64_t x = (static_cast<uint64_t>(idx) << 8) ^ static_cast<uint64_t>(used_len);
    // splitmix64 mix
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    x = x ^ (x >> 31);
    return x;
}

bool PrefixCache::Get(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    if (fast_dec) {
        int used_len = 0;
        uint32_t didx = 0;
        if (BuildIndexDec(key, len, used_len, didx)) {
            if (dec_cap == 0) return false;
            uint64_t h = HashDecKey(didx, static_cast<uint8_t>(used_len));
            size_t pos = static_cast<size_t>(h) & (dec_cap - 1);
            const size_t start = pos;
            while (true) {
                uint8_t st = dec_state[pos];
                if (st == 0) {
                    ++miss_count;
                    return false; // empty slot, not found
                }
                if (st == 1 && dec_lens[pos] == used_len && dec_keys[pos] == didx) {
                    out = dec_vals[pos];
                    ++hit_count;
                    return true;
                }
                pos = (pos + 1) & (dec_cap - 1);
                if (pos == start) {
                    // table fully scanned, not found
                    ++miss_count;
                    return false;
                }
            }
        }
        // fall through to other paths if non-decimal
    }
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
    if (fast_dec) {
        int used_len = 0;
        uint32_t didx = 0;
        if (BuildIndexDec(key, len, used_len, didx)) {
            if (dec_cap == 0) return;
            if (dec_size >= cap) {
                // Respect logical capacity; skip insertion to avoid overfilling
                return;
            }
            // Insert with linear probing; overwrite if exists
            uint64_t h = HashDecKey(didx, static_cast<uint8_t>(used_len));
            size_t pos = static_cast<size_t>(h) & (dec_cap - 1);
            const size_t start = pos;
            while (true) {
                uint8_t st = dec_state[pos];
                if (st == 0) {
                    // empty slot
                    dec_state[pos] = 1;
                    dec_keys[pos] = didx;
                    dec_lens[pos] = static_cast<uint8_t>(used_len);
                    dec_vals[pos] = in;
                    ++dec_size;
                    ++put_count;
                    return;
                }
                if (st == 1 && dec_lens[pos] == used_len && dec_keys[pos] == didx) {
                    dec_vals[pos] = in;
                    // do not change dec_size or put_count for overwrite
                    return;
                }
                pos = (pos + 1) & (dec_cap - 1);
                if (pos == start) {
                    // table full, cannot insert
                    return;
                }
            }
        }
        // fall through if non-decimal
    }
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
    if (fast_dec)
        return dec_size;
    if (fast4) {
        return static_cast<size_t>(std::count(filled4.begin(), filled4.end(), 1));
    }
    return map.size();
}

void PrefixCache::Clear()
{
    if (fast_dec) {
        std::fill(dec_state.begin(), dec_state.end(), 0);
        dec_size = 0;
    } else if (fast4) {
        std::fill(filled4.begin(), filled4.end(), 0);
    } else {
        map.clear();
    }
}

} // namespace mabain
