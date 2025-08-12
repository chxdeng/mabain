#include <assert.h>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "../db.h"

#include "./test_key.h"

using namespace mabain;

static const char* mbdir = "/var/tmp/mabain_test/";
static pthread_t wid = 0;
static MBConfig mbconf;
static bool stop_processing = false;
static uint32_t run_time = 3600;

static void* run_mb_test(void* arg);

static void* TestThread(void* arg)
{
    mbconf.options = CONSTS::ReaderOptions();
    DB db(mbconf);
    if (!db.is_open()) {
        std::cerr << "failed tp open db\n";
        abort();
    }
    assert(db.AsyncWriterEnabled());

    int64_t ikey = 0;
    int rval;
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey_sha1(MABAIN_TEST_KEY_TYPE_SHA_128);
    TestKey tkey_sha2(MABAIN_TEST_KEY_TYPE_SHA_256);
    std::string keystr;
    MBData mbd;

    while (!stop_processing) {
        keystr = tkey_int.get_key(ikey);
        rval = db.Find(keystr, mbd);
        if (rval != MBError::SUCCESS) {
            db.Add(keystr, keystr);
        }
        keystr = tkey_sha1.get_key(ikey);
        rval = db.Find(keystr, mbd);
        if (rval != MBError::SUCCESS) {
            db.Add(keystr, keystr);
        }
        keystr = tkey_sha2.get_key(ikey);
        rval = db.Find(keystr, mbd);
        if (rval != MBError::SUCCESS) {
            db.Add(keystr, keystr);
        }

        ikey++;
        usleep(1);
    }

    db.Close();
    return NULL;
}

void stop_mb_test()
{
    if (wid != 0) {
        if (pthread_join(wid, NULL) != 0) {
            std::cout << "cannot join mbtest thread\n";
        }
    }
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
    int nreaders = 8;
    srand(time(NULL));

    DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    memset(&mbconf, 0, sizeof(mbconf));
    mbconf.mbdir = mbdir;
    mbconf.block_size_index = 32 * 1024 * 1024;
    mbconf.block_size_data = 32 * 1024 * 1024;
    mbconf.memcap_index = 128LL * 1024 * 1024;
    mbconf.memcap_data = 128LL * 1024 * 1024;
    mbconf.num_entry_per_bucket = 500;

    int options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
    mbconf.options = options;
    DB* db = new DB(mbconf);
    if (!db->is_open()) {
        std::cerr << "failed to open writer db" << std::endl;
        delete db;
        abort();
    }

    pthread_t tid[256];
    assert(nreaders < 256);
    for (int i = 0; i < nreaders; i++) {
        if (pthread_create(&tid[i], NULL, TestThread, db) != 0) {
            std::cout << "failed to create reader thread" << std::endl;
            abort();
        }
    }

    int sleep_time;
    while (!stop_processing) {
        std::cout << "Running rc... " << db->Count() << "\n";
        db->CollectResource(32 * 1024 * 1024LL, 32 * 1024 * 1024LL, 134217728LL, 300000000000);

        sleep_time = (rand() % 10) + 1;
        sleep(sleep_time);

        if (time(NULL) >= run_stop_time) {
            stop_processing = true;
        }
    }

    for (int i = 0; i < nreaders; i++) {
        pthread_join(tid[i], NULL);
    }
    db->Close();
    delete db;
    DB::CloseLogFile();
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
        mbdir = argv[1];
        std::cout << "Test db directory is " << mbdir << "\n";
    }
    if (argc > 2) {
        run_time = atoi(argv[2]);
        std::cout << "running " << argv[0] << " for " << run_time << " seconds...\n";
    }

    DB::SetLogFile(std::string(mbdir) + "/mabain.log");
    DB::LogDebug();
    SetTestStatus(false);
    run_mb_test(NULL);
    DB::CloseLogFile();
    SetTestStatus(true);
    return 0;
}
