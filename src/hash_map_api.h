/**
 * Public Mabain HashMap API.
 */

#ifndef MABAIN_HASH_MAP_API_H
#define MABAIN_HASH_MAP_API_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include "mabain_consts.h"
#include "mb_data.h"

namespace mabain {

class HashMapImpl;

struct HashMapValueConfig {
    size_t value_block_size = 16ULL * 1024ULL * 1024ULL;
    size_t value_memcap = 256ULL * 1024ULL * 1024ULL;
    uint32_t reader_slots = 64;
    size_t reclaim_threshold_bytes = 1ULL * 1024ULL * 1024ULL;
    size_t max_retired_bytes = 64ULL * 1024ULL * 1024ULL;
    uint32_t max_load_percent = 85;
};

class HashMap {
public:
    // Create or open reference-storage mode. One writer process and multiple
    // reader processes may use the map. Opening a writer resets an existing
    // compatible map; all processes must use matching layout arguments.
    HashMap(const std::string& mbdir, size_t capacity, int options,
        uint32_t num_stripes = 64, uint32_t inline_key = 16,
        size_t memcap_mb = 32, bool compact64 = false);

    // Create or open value-storage mode. Immutable key/value records are held
    // in generation-named jemalloc files separate from the fixed index.
    HashMap(const std::string& mbdir, size_t capacity, int options,
        const HashMapValueConfig& config, size_t index_memcap_mb = 32);
    ~HashMap();

    HashMap(const HashMap&) = delete;
    HashMap& operator=(const HashMap&) = delete;
    HashMap(HashMap&&) noexcept;
    HashMap& operator=(HashMap&&) noexcept;

    int Put(const uint8_t* key, int len, size_t ref_offset,
        bool overwrite = true);
    bool Get(const uint8_t* key, int len, size_t& ref_offset) const;

    int PutValue(const uint8_t* key, int key_len, const uint8_t* value,
        int value_len, bool overwrite = true);
    int GetValue(const uint8_t* key, int key_len, MBData& value) const;

    int Erase(const uint8_t* key, int len);
    void PrintStats(std::ostream& os) const;
    void Flush() const;

private:
    std::unique_ptr<HashMapImpl> impl_;
};

} // namespace mabain

#endif // MABAIN_HASH_MAP_API_H
