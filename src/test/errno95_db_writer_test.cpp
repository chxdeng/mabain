#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "../db.h"
#include "../error.h"

using namespace mabain;

int main(int argc, char* argv[])
{
    const int64_t mb = 1024LL * 1024;
    static const char kDefaultDbDir[] = "/tmp/mabain_errno95_db";
    const char* db_dir = (argc > 1) ? argv[1] : kDefaultDbDir;
    int num_entries = (argc > 2) ? atoi(argv[2]) : 128;
    if (num_entries <= 0)
        num_entries = 1;
    int64_t default_memcap_mb = num_entries / 8000;
    if (default_memcap_mb < 4)
        default_memcap_mb = 4;
    int64_t memcap_index_mb = (argc > 3) ? atoll(argv[3]) : default_memcap_mb;
    int64_t memcap_data_mb = (argc > 4) ? atoll(argv[4]) : default_memcap_mb;
    if (memcap_index_mb < 4)
        memcap_index_mb = 4;
    if (memcap_data_mb < 4)
        memcap_data_mb = 4;

    int64_t block_index_mb = memcap_index_mb / 8;
    int64_t block_data_mb = memcap_data_mb / 8;
    if (block_index_mb < 4)
        block_index_mb = 4;
    if (block_data_mb < 4)
        block_data_mb = 4;
    if (block_index_mb > 64)
        block_index_mb = 64;
    if (block_data_mb > 64)
        block_data_mb = 64;
    try {
        if (argc <= 1)
            std::filesystem::remove_all(db_dir);
        std::filesystem::create_directories(db_dir);
    } catch (const std::filesystem::filesystem_error& ex) {
        std::cerr << "Failed to prepare db dir " << db_dir << ": " << ex.what() << std::endl;
        return 1;
    }


    DB::SetLogFile(std::string(db_dir) + "/mabain.log");
    {
        std::ofstream log(std::string(db_dir) + "/mabain.log", std::ios::out | std::ios::app);
        if (log.is_open())
            log << "errno95_db_writer_test db_dir=" << db_dir << std::endl;
    }

    MBConfig conf;
    memset(&conf, 0, sizeof(conf));
    conf.mbdir = db_dir;
    conf.options = CONSTS::WriterOptions();
    conf.memcap_index = memcap_index_mb * mb;
    conf.memcap_data = memcap_data_mb * mb;
    conf.block_size_index = block_index_mb * mb;
    conf.block_size_data = block_data_mb * mb;
    conf.queue_dir = db_dir;

    DB db(conf);
    if (!db.is_open()) {
        std::cerr << "DB open failed: " << db.StatusStr() << std::endl;
        DB::CloseLogFile();
        return 2;
    }

    for (int i = 0; i < num_entries; i++) {
        std::ostringstream key;
        std::ostringstream value;
        key << "errno95_probe_key_" << i;
        value << "errno95_probe_value_" << i;
        int rc = db.Add(key.str(), value.str());
        if (rc != MBError::SUCCESS) {
            std::cerr << "Add failed at i=" << i << ", rc=" << rc << std::endl;
            db.Close();
            DB::CloseLogFile();
            return 3;
        }
    }
    db.Close();

    MBConfig reader_conf = conf;
    reader_conf.options = CONSTS::ReaderOptions();
    DB reader(reader_conf);
    if (!reader.is_open()) {
        std::cerr << "Reader open failed: " << reader.StatusStr() << std::endl;
        DB::CloseLogFile();
        return 4;
    }

    for (int i = 0; i < num_entries; i++) {
        std::ostringstream key;
        std::ostringstream expected;
        key << "errno95_probe_key_" << i;
        expected << "errno95_probe_value_" << i;

        MBData data;
        int rc = reader.Find(key.str(), data);
        if (rc != MBError::SUCCESS) {
            std::cerr << "Find failed at i=" << i << ", rc=" << rc << std::endl;
            reader.Close();
            DB::CloseLogFile();
            return 5;
        }

        std::string actual(reinterpret_cast<const char*>(data.buff), data.data_len);
        if (actual != expected.str()) {
            std::cerr << "Value mismatch at i=" << i << std::endl;
            reader.Close();
            DB::CloseLogFile();
            return 6;
        }
    }

    reader.Close();
    DB::CloseLogFile();

    std::cout << "errno95_db_writer_test passed with " << num_entries
              << " insert/lookup operations"
              << " (memcap_index_mb=" << memcap_index_mb
              << ", memcap_data_mb=" << memcap_data_mb
              << ")" << std::endl;
    return 0;
}
