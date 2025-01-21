#include "db.h" // Include the header for the Mabain DB class
#include "test_key.h" // Include the header for the TestKey class
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <openssl/sha.h>
#include <queue>
#include <string>
#include <thread>
#include <unistd.h>

struct PruneData {
    std::mutex mtx;
    std::condition_variable cv;
    std::queue<std::string> prune_list;
    double prune_ratio;
    int64_t memcap_index;
    int64_t memcap_data;
    size_t prune_node_offset; // current offset of the node to be pruned
    uint32_t previous_node_offset; // previous offset of the node to be pruned
    int prune_cnt;
    PruneData()
    {
        prune_ratio = 0.5;
        prune_node_offset = 0xFFFFFFFFFFFFFFFF;
        previous_node_offset = 0;
        prune_cnt = 10;
    }
};

static bool need_to_prune(const mabain::DB& db, const PruneData& pdata)
{
    int64_t alloc_size = db.GetPendingIndexBufferSize();
    if (alloc_size > pdata.memcap_index * pdata.prune_ratio) {
        return true;
    }
    alloc_size = db.GetPendingDataBufferSize();
    if (alloc_size > pdata.memcap_data * pdata.prune_ratio) {
        return true;
    }
    return false;
}

int perform_prune(mabain::DB& db, PruneData& pdata)
{
    int cnt = 0;
    mabain::MBData mbd;
    while (cnt < pdata.prune_cnt) {
        mbd.Clear();
        // Load data using offset
        int rc = db.ReadDataByOffset(pdata.prune_node_offset, mbd);
        assert(rc == mabain::MBError::SUCCESS);
        // update the offset of the next node to be pruned
        pdata.prune_node_offset = *reinterpret_cast<uint32_t*>(mbd.buff);

        // remove the node
        rc = db.Remove((char*)mbd.buff + 4, mbd.data_len - 4 - 1); // -1 for the null ending
        if (rc != mabain::MBError::SUCCESS) {
            std::cerr << "Failed to remove key: " << (char*)mbd.buff + 4 << ", error: " << rc << std::endl;
        }

        assert(rc == mabain::MBError::SUCCESS);
        cnt++;
    }
    return cnt;
}

void construct_data(const std::string& key, mabain::MBData& data, PruneData& pdata)
{
    // calculate data length first
    // 4 byte offset + string + 1
    data.data_len = 4 + key.size() + 1;
    data.Resize(data.data_len);
    // write the previous offset
    uint32_t* offset_ptr = reinterpret_cast<uint32_t*>(data.buff);
    *offset_ptr = pdata.previous_node_offset;
    snprintf(reinterpret_cast<char*>(data.buff + 4), key.size() + 1, "%s", key.c_str());
}

void add_random_key_value_pairs(mabain::DB& db, int num_pairs, int key_type, PruneData& pdata)
{
    // Create a TestKey instance with the desired key type
    TestKey testKey(key_type);
    mabain::MBData data;

    // Add random key/value pairs to the database
    for (int i = 0; i < num_pairs; ++i) {
        // Generate a random key using TestKey
        std::string key = testKey.get_key(std::rand());
        // Generate a random value of random size between 10 and 100 bytes
        int value_size = 10 + std::rand() % 91;
        std::string value(value_size, ' ');
        for (int j = 0; j < value_size; ++j) {
            value[j] = 'A' + std::rand() % 26; // Random character between 'A' and 'Z'
        }

        // Construct the data buffer
        construct_data(key, data, pdata);
        // Add the key/value pair to the database
        int rc = db.Add(key.c_str(), key.size(), data, false);
        if (rc == mabain::MBError::SUCCESS) {
            if (pdata.prune_node_offset == 0xFFFFFFFFFFFFFFFF) {
                // This is the first add
                pdata.prune_node_offset = data.data_offset; // Save the offset of the first node
                std::cout << "first key: " << key << ", data len: " << data.data_len << std::endl;
            } else {
                // link previous node to the current node
                db.WriteDataByOffset(pdata.previous_node_offset, reinterpret_cast<const char*>(&data.data_offset), 4);
            }
            // Save the offset for next add
            pdata.previous_node_offset = data.data_offset + mabain::DB::GetDataHeaderSize();
            if (need_to_prune(db, pdata)) {
                perform_prune(db, pdata);
            }
        } else {
            if (rc == mabain::MBError::IN_DICT) {
                // handle cache update
            } else {
                std::cerr << "Failed to add key: " << key << ", error: " << rc << std::endl;
            }
        }
    }
    std::cout << "current db count " << db.Count() << std::endl;
    std::cout << "current index buffer size " << db.GetPendingIndexBufferSize() << std::endl;
    std::cout << "current data buffer size " << db.GetPendingDataBufferSize() << std::endl;
}

int main(int argc, char* argv[])
{
    // Initialize random seed
    std::srand(std::time(nullptr));

    // Default values
    int num_pairs = 1000;
    int key_type = MABAIN_TEST_KEY_TYPE_INT;
    int iter = 2;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-n" && i + 1 < argc) {
            num_pairs = std::stoi(argv[++i]);
        } else if (arg == "-k" && i + 1 < argc) {
            std::string key_type_str = argv[++i];
            if (key_type_str == "int") {
                key_type = MABAIN_TEST_KEY_TYPE_INT;
            } else if (key_type_str == "sha1") {
                key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
            } else if (key_type_str == "sha2") {
                key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
            } else {
                std::cerr << "Unknown key type: " << key_type_str << std::endl;
                return 1;
            }
        } else if (arg == "-iter" && i + 1 < argc) {
            iter = std::stoi(argv[++i]);
        }
    }

    //mabain::DB::LogDebug();
    // Create a Mabain DB instance
    mabain::MBConfig config;
    memset(&config, 0, sizeof(config));
    config.mbdir = "/var/tmp/mabain_test";
    config.options = mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC;
    config.block_size_index = 32 * 1024 * 1024;
    config.block_size_data = 32 * 1024 * 1024;
    config.max_num_data_block = 3;
    config.max_num_index_block = 3;
    config.memcap_index = config.block_size_index * config.max_num_index_block;
    config.memcap_data = config.block_size_data * config.max_num_data_block;
    mabain::DB db_writer(config);
    assert(db_writer.Status() == mabain::MBError::SUCCESS);

    PruneData PDATA;
    PDATA.memcap_index = config.memcap_index;
    PDATA.memcap_data = config.memcap_data;

    for (int i = 0; i < iter; ++i) {
        std::cout << "test iteration: " << i << std::endl;
        add_random_key_value_pairs(db_writer, num_pairs, key_type, PDATA);
        db_writer.Purge();
    }

    db_writer.PrintStats();
    return 0;
}
