#include <assert.h>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>

#include "../db.h"
#include "./test_key.h"

using namespace mabain;

int db_ref_count = 0;
DB* db_writer = nullptr;
std::mutex writer_lock;
std::mutex init_lock;
int num = 1000000;

DB* OpenDB(bool is_writer, const char* db_dir)
{
    DB* db = nullptr;
    if (is_writer) {
        writer_lock.lock();
        if (!db_ref_count) {
            MBConfig writer_config = { 0 };
            writer_config.mbdir = db_dir;
            writer_config.options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
            writer_config.memcap_index = 256 * 1024 * 1024LL;
            writer_config.memcap_data = 256 * 1024 * 1024LL;
            writer_config.num_entry_per_bucket = 500;
            db_writer = new DB(writer_config);
            if (!db_writer->is_open()) {
                std::cerr << "Failed to open mabain db writer: " << db_writer->StatusStr() << "\n";
                delete db_writer;
                exit(1);
            }
            //db_writer->LogDebug();
            db_writer->CollectResource(1, 1, 0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF);
            db_ref_count = 1;
        } else {
            std::cout << "Mabain db writer is already opened\n";
            db_ref_count++;
        }
        writer_lock.unlock();
    }

    MBConfig reader_config = { 0 };
    reader_config.mbdir = db_dir;
    reader_config.options = CONSTS::ReaderOptions();
    reader_config.memcap_index = 256 * 1024 * 1024;
    reader_config.memcap_data = 256 * 1024 * 1024;
    reader_config.num_entry_per_bucket = 500;
    db = new DB(reader_config);
    if (!db->is_open()) {
        std::cerr << "Failed to open mabain db reader: " << db->StatusStr() << "\n";
        delete db;
        exit(1);
    }
    return db;
}

#define DB_DIR "/var/tmp/mabain_test"
DB* Initialize()
{
    DB* db = nullptr;
    init_lock.lock();
    db = OpenDB(true, DB_DIR);
    init_lock.unlock();
    return db;
}

void thread_test()
{
    DB* db = Initialize();
    TestKey tkey_int(MABAIN_TEST_KEY_TYPE_INT);
    std::string keystr;
    MBData mbd;
    int cnt = 0;
    std::cout << "===========\n";
    for (int i = 0; i < num; i++) {
        keystr = tkey_int.get_key(i);
        if (db->Find(keystr, mbd) != MBError::SUCCESS) {
            if (db->AddAsync(keystr.data(), keystr.size(), keystr.data(), keystr.size()) != MBError::SUCCESS)
                cnt++;
        }
    }
    std::cout << "failed count: " << cnt << "\n";
    std::cout << "===========\n";
    delete db;
}

static void overwrite_test()
{
    DB* db = Initialize();
    std::string key = "This key is used to test overwrite!";
    std::string val0 = "This is the original value";
    assert(MBError::SUCCESS == db->AddAsync(key.data(), key.size(), val0.data(), val0.size(), true));
    MBData mbd;
    assert(MBError::SUCCESS == db->Find(key, mbd));
    assert(val0 == std::string((const char*)mbd.buff, mbd.data_len));
    std::string val1 = "This is the updated value";
    assert(MBError::SUCCESS == db->AddAsync(key.data(), key.size(), val1.data(), val1.size(), true));
    sleep(1); // wait for the async writer to update it
    assert(MBError::SUCCESS == db->Find(key, mbd));
    assert(val1 == std::string((const char*)mbd.buff, mbd.data_len));
}

int main()
{
    int nthread = 32;
    std::thread thr[256];
    assert(nthread <= 256);

    DB::SetLogFile(std::string(DB_DIR) + "/mabain.log");
    for (int i = 0; i < nthread; i++) {
        thr[i] = std::thread(thread_test);
    }
    for (int i = 0; i < nthread; i++) {
        thr[i].join();
    }

    overwrite_test();

    delete db_writer;
    DB::CloseLogFile();
    return 0;
}
