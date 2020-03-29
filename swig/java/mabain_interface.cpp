#include <cassert>

#include "mabain/mb_data.h"
#include "mabain/error.h"
#include "mabain_interface.h"

using namespace std;
using namespace mabain;

Mabain::Mabain(const string &db_path, bool writer_mode, bool async_mode = false)
         : dbi(nullptr)
{
    if (async_mode) assert(writer_mode);

    int mode = 0;
    if (writer_mode) mode |= CONSTS::WriterOptions();
    if (async_mode)  mode |= CONSTS::ASYNC_WRITER_MODE;
    try {
        dbi = new DB(db_path.c_str(), mode, 9223372036854775807LL, 9223372036854775807LL);
    } catch (int error) {
        cerr << "failed to open DB in " << db_path << ": "
             << MBError::get_error_str(error) << endl;
        assert(error == 0);
    }
}

Mabain::~Mabain()
{
    if (dbi != nullptr) delete dbi;
}

int Mabain::mbAdd(const std::string key, const std::string val)
{
    if (dbi == nullptr) return -1;
    return dbi->Add(key, val);
}

int Mabain::mbDelete(const std::string key)
{
    if (dbi == nullptr) return -1;
    return dbi->Remove(key);
}

int Mabain::mbFind(const std::string key, mb_query_result &result)
{
    if (dbi == nullptr) return -1;
    MBData mbd;
    int rval = dbi->Find(key, mbd);
    if (rval == MBError::SUCCESS) {
        result.data = (char*) mbd.buff;
        result.len  = mbd.data_len;
        result.data[mbd.data_len] = '\0';
        mbd.buff = nullptr;
    }
    return rval;
}

int Mabain::mbGC(long long max_key_size, long long max_val_size)
{
    if (dbi == nullptr) return -1;
    return dbi->CollectResource(max_key_size, max_val_size);
}
