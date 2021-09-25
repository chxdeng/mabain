#ifndef __APPEND_H__
#define __APPEND_H__

#include "mb_data.h"
#include "dict.h"

namespace mabain {

class Append {
public:
    Append(Dict &dict_ref, EdgePtrs &eptrs);
    ~Append();
    int AddDataBuffer(const uint8_t *buff, int buff_len);
    bool IsExistingKey() const;
    size_t GetOldDataOffset() const;

private:
    int AppendDataBuffer(size_t &data_offset, const uint8_t *buff, int buff_len);
    int AddDataToLeafNode(const uint8_t *buff, int buff_len);

    Dict &dict;
    EdgePtrs &edge_ptrs;
    bool existing_key;
    size_t old_data_offset; // if existing_key is true
};

}

#endif
