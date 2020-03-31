#include <cassert>

#include "mabain/mb_data.h"
#include "mabain/error.h"
#include "mabain_interface.h"

using namespace std;
using namespace mabain;

#define __MB_SIZE_UNLIMITED 9223372036854775807LL
Mabain::Mabain(const string &db_path, bool writer_mode, bool async_mode = false)
         : dbi(nullptr), db_async(nullptr)
{
    if (async_mode && !writer_mode) {
        cerr << "async mode can only be used with writer mode\n";
        return;
    }

    int mode = 0;
    if (writer_mode) mode |= CONSTS::WriterOptions();
    try {
        if (async_mode)  {
            db_async = new DB(db_path.c_str(), mode | CONSTS::ASYNC_WRITER_MODE,
                              __MB_SIZE_UNLIMITED, __MB_SIZE_UNLIMITED);
            mode = 0;
        }
        dbi = new DB(db_path.c_str(), mode, __MB_SIZE_UNLIMITED, __MB_SIZE_UNLIMITED);
    } catch (int error) {
        cerr << "failed to open DB in " << db_path << ": "
             << MBError::get_error_str(error) << endl;
    }
}

Mabain::~Mabain()
{
    if (dbi != nullptr)
        delete dbi;
    if (db_async != nullptr)
        delete db_async;
}

int Mabain::mbAdd(const std::string key, const std::string val)
{
    if (dbi == nullptr) return -1;
    int rval;
    try {
        rval = dbi->Add(key, val);
    } catch (int error) {
        rval = error;
    }
    return rval;
}

int Mabain::mbDelete(const std::string key)
{
    if (dbi == nullptr) return -1;
    int rval;
    try {
        rval = dbi->Remove(key);
    } catch (int error) {
        rval = error;
    }
    return rval;
}

int Mabain::mbFind(const std::string key, mb_query_result &result)
{
    if (dbi == nullptr) return -1;

    MBData mbd;
    int rval;
    try {
        rval = dbi->Find(key, mbd);
    } catch (int error) {
        rval = error;
    }
    if (rval == MBError::SUCCESS) {
        result.data = (char*) mbd.buff;
        result.len  = mbd.data_len;
        result.data[mbd.data_len] = '\0';
        mbd.buff = nullptr;
    }
    return rval;
}

bool Mabain::mbIsOpen() const
{
    if (dbi == nullptr) return false;
    return dbi->is_open();
}

int Mabain::mbGC(long long max_key_size, long long max_val_size)
{
    if (dbi == nullptr) return -1;

    int rval;
    try {
        rval = dbi->CollectResource(max_key_size, max_val_size);
    } catch (int error) {
        rval = error;
    }
    return rval;
}
