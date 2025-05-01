#include <cassert>
#include <cstdlib>
#include <map>
#include <sys/time.h>

#include "../db.h"

#include "./test_key.h"

using namespace std;
using namespace mabain;

static void InsertRandom(map<string, string>& m, DB* db, int num, int type)
{
    int mod_n = num * 10;
    string kv;
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    for (int i = 0; i < num; i++) {
        int val = rand() % mod_n;
        switch (type) {
        case 0:
            kv = to_string(val);
            break;
        case 1:
            kv = string((char*)&val, 4);
            break;
        case 2:
            kv = tkey_sha1.get_key(val);
            break;
        case 3:
            kv = tkey_sha2.get_key(val);
            break;
        }

        m[kv] = kv;
        db->Add(kv, kv);
    }
}

static int bound_key_err = 0;
static void find_test(map<string, string>& m, DB* db, int num, int* test_key, int type)
{
    MBData mbd;
    string kv;
    std::string bound_key;
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    for (int i = 0; i < num; i++) {
        mbd.Clear();
        bound_key.clear();

        switch (type) {
        case 0:
            kv = to_string(test_key[i]);
            break;
        case 1:
            kv = string((char*)&test_key[i], 4);
            break;
        case 2:
            kv = tkey_sha1.get_key(test_key[i]);
            break;
        case 3:
            kv = tkey_sha2.get_key(test_key[i]);
            break;
        }

        auto lower = m.lower_bound(kv);
        if (lower == m.end())
            continue;
        if (lower->first != kv)
            lower--;
        int rval = db->FindLowerBound(kv, mbd, &bound_key);
        if (lower != m.end()) {
            if (rval != MBError::SUCCESS || lower->first != string((char*)mbd.buff, mbd.data_len)) {
                cout << test_key[i] << " " << lower->first << " "
                     << string((char*)mbd.buff, mbd.data_len) << ": " << rval << "\n";
                abort();
            }
            // compare the constructed bound key in mabain
            if (bound_key != lower->first) {
                cout << "search key: " << test_key[i] << ", " << kv << ", lower->first: " << lower->first << ", bound key: "
                     << bound_key << ", value: " << string((char*)mbd.buff, mbd.data_len) << std::endl;
                abort();
                bound_key_err++;
            }
        }
    }

    timeval start, stop;
    uint64_t timediff;

    gettimeofday(&start, nullptr);
    for (int i = 0; i < num; i++) {
        switch (type) {
        case 0:
            kv = to_string(test_key[i]);
            break;
        case 1:
            kv = string((char*)&test_key[i], 4);
            break;
        case 2:
            kv = tkey_sha1.get_key(test_key[i]);
            break;
        case 3:
            kv = tkey_sha2.get_key(test_key[i]);
            break;
        }
        auto lower = m.lower_bound(kv);
        if (lower == m.end())
            continue;
        if (lower->first != kv)
            lower--;
        if (lower != m.end()) {
            assert(lower->first == lower->second);
        }
    }
    gettimeofday(&stop, nullptr);
    timediff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "map ===== " << timediff * 1.0 / num << " micro seconds per lookup\n";

    gettimeofday(&start, nullptr);
    for (int i = 0; i < num; i++) {
        mbd.Clear();
        switch (type) {
        case 0:
            kv = to_string(test_key[i]);
            break;
        case 1:
            kv = string((char*)&test_key[i], 4);
            break;
        case 2:
            kv = tkey_sha1.get_key(test_key[i]);
            break;
        case 3:
            kv = tkey_sha2.get_key(test_key[i]);
            break;
        }

        db->FindLowerBound(kv, mbd, nullptr);
    }
    gettimeofday(&stop, nullptr);
    timediff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "mabain ===== " << timediff * 1.0 / num << " micro seconds per lookup\n";
    std::cout << "bound key error count: " << bound_key_err << "\n";
}

int main()
{
    int num = 1000000;
    map<string, string> m;

    mabain::MBConfig mconf;
    memset(&mconf, 0, sizeof(mconf));
    std::string mbdir_tmp = "/var/tmp/mabain_test/";
    mconf.mbdir = mbdir_tmp.c_str();
    mconf.options = mabain::CONSTS::WriterOptions();
    mconf.options |= mabain::CONSTS::OPTION_JEMALLOC;
    mconf.block_size_index = 32 * 1024 * 1024;
    mconf.block_size_data = 32 * 1024 * 1024;
    mconf.memcap_index = 10 * mconf.block_size_index;
    mconf.memcap_data = 10 * mconf.block_size_data;
    mconf.max_num_index_block = 10;
    mconf.max_num_data_block = 10;
    DB* db = new DB(mconf);

    int* test_key = new int[num];
    int mod_n = num * 10;
    db->RemoveAll();
    srand(time(NULL));

    InsertRandom(m, db, num, 0);
    for (int i = 0; i < num; i++) {
        test_key[i] = rand() % mod_n;
    }
    find_test(m, db, num, test_key, 0);

    cout << "=========================\n";
    db->RemoveAll();
    m.clear();
    InsertRandom(m, db, num, 1);
    for (int i = 0; i < num; i++) {
        test_key[i] = rand() % mod_n;
    }
    find_test(m, db, num, test_key, 1);

    cout << "=========================\n";
    db->RemoveAll();
    m.clear();
    InsertRandom(m, db, num, 2);
    for (int i = 0; i < num; i++) {
        test_key[i] = rand() % mod_n;
    }
    find_test(m, db, num, test_key, 2);

    cout << "=========================\n";
    db->RemoveAll();
    m.clear();
    InsertRandom(m, db, num, 3);
    for (int i = 0; i < num; i++) {
        test_key[i] = rand() % mod_n;
    }
    find_test(m, db, num, test_key, 3);

    delete db;
    delete[] test_key;
    return 0;
}
