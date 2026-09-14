/**
 * Public HashMap API forwarding to the internal implementation.
 */

#include "hash_map_api.h"

#include "hash_map_internal.h"

namespace mabain {

HashMap::HashMap(const std::string& mbdir, size_t capacity, int options,
    uint32_t num_stripes, uint32_t inline_key, size_t memcap_mb,
    bool compact64)
    : impl_(new HashMapImpl(mbdir, capacity, options, num_stripes, inline_key,
        memcap_mb, compact64))
{
}

HashMap::HashMap(const std::string& mbdir, size_t capacity, int options,
    const HashMapValueConfig& config, size_t index_memcap_mb)
    : impl_(new HashMapImpl(
        mbdir, capacity, options, config, index_memcap_mb))
{
}

HashMap::~HashMap() = default;
HashMap::HashMap(HashMap&&) noexcept = default;
HashMap& HashMap::operator=(HashMap&&) noexcept = default;

int HashMap::Put(
    const uint8_t* key, int len, size_t ref_offset, bool overwrite)
{
    return impl_->Put(key, len, ref_offset, overwrite);
}

bool HashMap::Get(const uint8_t* key, int len, size_t& ref_offset) const
{
    return impl_->Get(key, len, ref_offset);
}

int HashMap::PutValue(const uint8_t* key, int key_len, const uint8_t* value,
    int value_len, bool overwrite)
{
    return impl_->PutValue(key, key_len, value, value_len, overwrite);
}

int HashMap::GetValue(const uint8_t* key, int key_len, MBData& value) const
{
    return impl_->GetValue(key, key_len, value);
}

int HashMap::Erase(const uint8_t* key, int len)
{
    return impl_->Erase(key, len);
}

void HashMap::PrintStats(std::ostream& os) const
{
    impl_->PrintStats(os);
}

void HashMap::Flush() const
{
    impl_->Flush();
}

} // namespace mabain
