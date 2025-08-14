#include <cassert>
#include <cstdlib>
#include <map>
#include <string>
#include <sys/time.h>
#include <vector>

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

static void find_test(map<string, string>& m, DB* db, int num, int* test_key, int type)
{
    MBData mbd;
    string kv;
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    // Precompute all query keys so timing measures only the lookup calls
    std::vector<std::string> queries;
    queries.reserve(num);
    for (int i = 0; i < num; i++) {
        switch (type) {
        case 0:
            queries.emplace_back(to_string(test_key[i]));
            break;
        case 1:
            queries.emplace_back(string((char*)&test_key[i], 4));
            break;
        case 2:
            queries.emplace_back(tkey_sha1.get_key(test_key[i]));
            break;
        case 3:
            queries.emplace_back(tkey_sha2.get_key(test_key[i]));
            break;
        }
    }

    // Optional correctness check (not timed): verify DB lowerBound equals std::map lower_bound
    for (int i = 0; i < num; i++) {
        auto lower = m.lower_bound(queries[i]);
        if (lower == m.end())
            continue;
        if (lower->first != queries[i])
            lower--;
        if (lower == m.end())
            continue;
        mbd.Clear();
        int rv = db->FindLowerBound(queries[i], mbd, nullptr);
        if (rv != MBError::SUCCESS || lower->first != string((char*)mbd.buff, mbd.data_len)) {
            std::cerr << "mismatch for " << queries[i] << std::endl;
            abort();
        }
    }

    // Timed region 1: std::map lower_bound only (no key construction)
    timeval start, stop;
    volatile size_t sink = 0; // prevent optimizer from eliding work
    gettimeofday(&start, nullptr);
    for (int i = 0; i < num; i++) {
        auto it = m.lower_bound(queries[i]);
        if (it != m.end())
            sink += it->first.size();
    }
    gettimeofday(&stop, nullptr);
    uint64_t timediff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "map "
              << (type == 0 ? "type0 " : type == 1 ? "type1 "
                         : type == 2               ? "type2 "
                                                   : "type3 ")
              << timediff * 1.0 / num << " micro seconds per lookup\n";

    // Timed region 2: only the actual DB lookup calls
    gettimeofday(&start, nullptr);
    for (int i = 0; i < num; i++) {
        // Only measure traversal cost; skip value read
        mbd.data_len = 0;
        mbd.match_len = 0;
        mbd.options = CONSTS::OPTION_KEY_ONLY;
        db->FindLowerBound(queries[i], mbd, nullptr);
    }
    gettimeofday(&stop, nullptr);
    timediff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "mabain "
              << (type == 0 ? "type0 " : type == 1 ? "type1 "
                         : type == 2               ? "type2 "
                                                   : "type3 ")
              << timediff * 1.0 / num << " micro seconds per lookup\n";
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
