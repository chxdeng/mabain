#ifndef __MABAIN_INTERFACE_H__
#define __MABAIN_INTERFACE_H__

#include <string>

#include "mabain/db.h"

class mb_query_result {
public:
    char *data;
    int len;

    mb_query_result() {
        data = NULL;
        len = 0;
    }
    ~mb_query_result() {
        if(data != NULL) free(data);
    }
    char* get() {
        return data;
    }
};

class Mabain {
public:
    Mabain(const std::string &db_path, bool writer_mode, bool async_mode); 
    int mbAdd(const std::string key, const std::string val);
    int mbFind(const std::string key, mb_query_result &result);
    int mbDelete(const std::string key);
    // mabain GC
    // max_key_size: pending key size to trigger GC on index DB
    // max_val_size: pending value size to trigger GC on value DB
    int mbGC(long long max_key_size, long long max_val_size);
    ~Mabain();

private:
    mabain::DB *dbi;    
};

#endif
