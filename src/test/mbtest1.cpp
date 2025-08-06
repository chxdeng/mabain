#include <assert.h>
#include <atomic>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <filesystem>

#include "../db.h"

#include "./test_key.h"

using namespace mabain;

static std::atomic<int64_t> key_low;
static std::atomic<int64_t> key_high;
static int64_t memcap_i = 256 * 1024 * 1024;
static int64_t memcap_d = 256 * 1024 * 1024;
static pthread_t wid = 0;

static std::string mbdir = "/var/tmp/mabain_test";
static bool stop_processing = false;
static uint32_t run_time = 3600;

static void* run_mb_test(void* arg);

static void* Reader(void* arg)
{
    DB db(mbdir.c_str(), CONSTS::ReaderOptions(), memcap_i, memcap_d);
    if (!db.is_open()) {
        std::cerr << "failed tp open db\n";
        abort();
    }

    int64_t ikey;
    int rval;
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    std::string keystr;
    MBData mbd;
    int ktype;

    while (!stop_processing) {
        // usleep(5);

        if (key_high.load(std::memory_order_consume) == key_low.load(std::memory_order_consume))
            continue;
        ikey = key_low.load(std::memory_order_consume);
        ikey += rand() % (key_high.load(std::memory_order_consume) - key_low.load(std::memory_order_consume));

        ktype = rand() % 3;
        switch (ktype) {
        case 0:
            keystr = tkey_int.get_key(ikey);
            break;
        case 1:
            keystr = tkey_sha1.get_key(ikey);
            break;
        case 2:
            keystr = tkey_sha2.get_key(ikey);
            break;
        }
        rval = db.Find(keystr, mbd);

        if (ikey < key_low.load(std::memory_order_consume))
            continue;

        if (rval == MBError::SUCCESS) {
            if (keystr != std::string((char*)mbd.buff, mbd.data_len)) {
                std::cout << "value not match for key " << ikey << ": " << keystr << "\n";
                abort();
            }
        } else if (rval != MBError::NOT_EXIST) {
            std::cout << "unexpected return from Find: " << rval << "\n";
            abort();
        }
    }

    db.Close();
    return NULL;
}

static void Verify(DB& db)
{
    int64_t key0 = key_low.load(std::memory_order_consume);
    int64_t key1 = key_high.load(std::memory_order_consume);
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    MBData mbd;
    std::string keystr;
    for (int64_t i = key0; i < key1; i++) {
        keystr = tkey_int.get_key(i);
        if (db.Find(keystr, mbd) != MBError::SUCCESS) {
            std::cout << "failed to find key " << i << ": " << keystr << std::endl;
            abort();
        }
        assert(keystr == std::string((char*)mbd.buff, mbd.data_len));

        keystr = tkey_sha1.get_key(i);
        if (db.Find(keystr, mbd) != MBError::SUCCESS) {
            std::cout << "failed to find key " << i << ": " << keystr << std::endl;
            abort();
        }
        assert(keystr == std::string((char*)mbd.buff, mbd.data_len));

        keystr = tkey_sha2.get_key(i);
        if (db.Find(keystr, mbd) != MBError::SUCCESS) {
            std::cout << "failed to find key " << i << ": " << keystr << std::endl;
            abort();
        }
        assert(keystr == std::string((char*)mbd.buff, mbd.data_len));
    }
}

static void* AddThread(void* arg)
{
    DB db(mbdir.c_str(), CONSTS::ReaderOptions());
    int num = rand() % 1500;

    int64_t key;
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    std::string keystr;
    int rval;

    for (int i = 0; i < num; i++) {
        key = key_high.fetch_add(1, std::memory_order_release);
        keystr = tkey_int.get_key(key);
        rval = db.Add(keystr, keystr);
        assert(rval == MBError::SUCCESS || rval == MBError::TRY_AGAIN);

        keystr = tkey_sha1.get_key(key);
        rval = db.Add(keystr, keystr);
        assert(rval == MBError::SUCCESS || rval == MBError::TRY_AGAIN);

        keystr = tkey_sha2.get_key(key);
        rval = db.Add(keystr, keystr);
        assert(rval == MBError::SUCCESS || rval == MBError::TRY_AGAIN);
    }
    db.Close();
    return NULL;
}

static void Populate(int nt)
{
    pthread_t tid[256];
    assert(nt < 256);
    for (int i = 0; i < nt; i++) {
        if (pthread_create(&tid[i], NULL, AddThread, NULL) != 0) {
            std::cout << "failed to create MultiThreadAdd thread\n";
            abort();
        }
    }

    for (int i = 0; i < nt; i++) {
        if (pthread_join(tid[i], NULL) != 0) {
            std::cout << "failed to join MultiThreadAdd thread\n";
            abort();
        }
    }
}

static void load_key_ids()
{
    int64_t klow, khigh;
    std::ifstream ifs;
    std::string path = mbdir + "/key_id";
    ifs.open(path.c_str(), std::ofstream::in);
    if (ifs.is_open()) {
        ifs >> klow;
        ifs >> khigh;
        ifs.close();
    } else {
        klow = 0;
        khigh = 0;
    }
    key_low = klow;
    key_high = khigh;
    std::cout << "Loaded " << key_low << " " << key_high << "\n";
}

static void store_key_ids()
{
    std::string path = mbdir + "/key_id";
    FILE* fp;
    fp = fopen(path.c_str(), "w");
    if (!fp)
        return;
    fprintf(fp, "%d\n%d", (int)key_low.load(std::memory_order_consume),
        (int)key_high.load(std::memory_order_consume));
    fclose(fp);
}

static void* DeleteThread(void* arg)
{
    DB db(mbdir.c_str(), CONSTS::ReaderOptions());
    int num = rand() % 5;

    int64_t key;
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    std::string keystr;

    for (int i = 0; i < num; i++) {
        key = key_low.fetch_add(1, std::memory_order_release);

        keystr = tkey_int.get_key(key);
        db.Remove(keystr);

        keystr = tkey_sha1.get_key(key);
        db.Remove(keystr);

        keystr = tkey_sha2.get_key(key);
        db.Remove(keystr);
    }

    db.Close();
    return NULL;
}

static void Prune(int nt)
{
    pthread_t tid[256];
    assert(nt < 256);
    for (int i = 0; i < nt; i++) {
        if (pthread_create(&tid[i], NULL, DeleteThread, NULL) != 0) {
            std::cout << "failed to create MultiThreadAdd thread\n";
            abort();
        }
    }

    for (int i = 0; i < nt; i++) {
        if (pthread_join(tid[i], NULL) != 0) {
            std::cout << "failed to join MultiThreadAdd thread\n";
            abort();
        }
    }
}

void stop_mb_test()
{
    if (wid != 0) {
        if (pthread_join(wid, NULL) != 0) {
            std::cout << "cannot join mbtest thread\n";
        }
    }
}

static void CheckCount()
{
    DB* db = new DB(mbdir.c_str(), CONSTS::ReaderOptions(), memcap_i, memcap_d);
    if (db == NULL)
        return;

    int64_t count = 0;
    for (DB::iterator iter = db->begin(false, false); iter != db->end(); ++iter) {
        count++;
    }
    std::cout << "Count using iterator: " << count << "\tcount from API: "
              << db->Count() << "\n";
    delete db;
}

void start_mb_test()
{
    if (pthread_create(&wid, NULL, run_mb_test, NULL) != 0) {
        std::cout << "failed to create test thread" << std::endl;
    }
}

static void* run_mb_test(void* arg)
{
    int64_t run_stop_time = time(NULL) + run_time;
    int nreaders = 4;
    int nupdates = 4;
    srand(time(NULL));

    load_key_ids();

    MBConfig mbconf;
    int options;
    memset(&mbconf, 0, sizeof(mbconf));
    mbconf.mbdir = mbdir.c_str();
    options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
    mbconf.options = options;
    mbconf.memcap_index = 128ULL * 1024 * 1024;
    mbconf.memcap_data = 128ULL * 1024 * 1024;
    mbconf.block_size_index = 64U * 1024 * 1024;
    mbconf.block_size_data = 64U * 1024 * 1024;
    mbconf.max_num_data_block = 3;
    mbconf.max_num_index_block = 3;
    mbconf.num_entry_per_bucket = 500;
    DB* db = new DB(mbconf);
    if (!db->is_open()) {
        std::cerr << "failed to open writer db" << std::endl;
        delete db;
        abort();
    }

    pthread_t tid;
    for (int i = 0; i < nreaders; i++) {
        if (pthread_create(&tid, NULL, Reader, NULL) != 0) {
            std::cout << "failed to create reader thread" << std::endl;
            abort();
        }
    }

    int rcn = 0;
    int64_t loop_cnt = 0;
    while (!stop_processing) {
        Populate(nupdates);
        Prune(nupdates);

        std::cout << "LOOP " << loop_cnt << ": " << db->Count() << "\n";
        if (loop_cnt % 5 == 0) {
            std::cout << "RUN RC\n";
            int rval = db->CollectResource(24LL * 1024 * 1024, 24LL * 1024 * 1024, 128LL * 1024 * 1024, 0xFFFFFFFFFFFF);
            if (rval == MBError::SUCCESS) {
                rcn++;
                if (rcn % 57 == 0 && !(options & CONSTS::ASYNC_WRITER_MODE)) {
                    // Note if in async mode, the DB handle cannot be used for lookup.
                    std::cout << "Verifying after rc" << std::endl;
                    Verify(*db);
                } else if (rcn % 20 == 0) {
                    db->Close();
                    delete db;
                    mbconf.options = options;
                    db = new DB(mbconf);
                    if (!db->is_open()) {
                        delete db;
                        abort();
                    }
                    CheckCount();
                } else if (rcn % 20000523 == 0) {
                    // Skip reboot command - this was a system test feature
                    // if (system("reboot") != 0) {
                    // }
                }
            }
        }

        sleep(1);
        loop_cnt++;
        if (time(NULL) >= run_stop_time) {
            stop_processing = true;
        }
    }

    db->Close();
    delete db;
    store_key_ids();

    return NULL;
}

static void SetTestStatus(bool success)
{
    std::string success_file = std::string(mbdir) + "/_success";
    
    try {
        if (success) {
            std::ofstream file(success_file);
            file.close();
        } else {
            std::filesystem::remove(success_file);
        }
    } catch (const std::filesystem::filesystem_error& ex) {
        // Ignore errors, similar to original system() calls
    }
}

int main(int argc, char* argv[])
{
    if (argc > 1) {
        mbdir = std::string(argv[1]);
        std::cout << "Test db directory is " << mbdir << "\n";
    }
    if (argc > 2) {
        run_time = atoi(argv[2]);
        std::cout << "running " << argv[0] << " for " << run_time << " seconds...\n";
    }

    DB::SetLogFile(mbdir + "/mabain.log");
    SetTestStatus(false);
    run_mb_test(NULL);
    SetTestStatus(true);
    DB::CloseLogFile();
    return 0;
}
