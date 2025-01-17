#include "db.h" // Include the header for the Mabain DB class
#include "test_key.h" // Include the header for the TestKey class
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <openssl/sha.h>
#include <string>

void add_random_key_value_pairs(mabain::DB& db, int num_pairs, int key_type)
{
    // Create a TestKey instance with the desired key type
    TestKey testKey(key_type);
    std::vector<std::string> keys;

    // Add random key/value pairs to the database
    for (int i = 0; i < num_pairs; ++i) {
        // Generate a random key using TestKey
        const char* key = testKey.get_key(std::rand());

        // Add the key/value pair to the database
        db.Add(key, key);
        keys.push_back(key);
    }

    for (unsigned i = 0; i < keys.size(); ++i) {
        // Remove the key/value pair from the database
        db.Remove(keys[i]);
    }
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
    config.memcap_index = 128 * 1024 * 1024;
    config.memcap_data = 128 * 1024 * 1024;
    config.block_size_index = config.memcap_index;
    config.block_size_data = config.memcap_data;
    config.max_num_data_block = 1;
    config.max_num_index_block = 1;
    mabain::DB db(config);
    assert(db.Status() == mabain::MBError::SUCCESS);

    // Add/remove random key/value pairs to the database
    for (int i = 0; i < iter; ++i) {
        std::cout << "test iteration: " << i << std::endl;
        add_random_key_value_pairs(db, num_pairs, key_type);
        if ((i + 1) % 10 == 0)
            db.Purge();
    }
    db.PrintStats();

    return 0;
}