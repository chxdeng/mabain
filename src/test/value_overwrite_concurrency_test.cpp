/**
 * Verify that a lookup concurrent with a same-size value overwrite returns
 * either the complete old value or the complete new value, never a mixture.
 */

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "db.h"
#include "error.h"
#include "mabain_consts.h"

using namespace mabain;

namespace {

const char* const kDbDir = "/var/tmp/mabain_value_overwrite_concurrency";
const char* const kKey = "value-overwrite-key";
const size_t kValueSize = 32000;
const uint32_t kBlockSize = 16 * 1024 * 1024;
const int kMaxBlocks = 4;

MBConfig MakeConfig(int options)
{
    MBConfig config = {};
    config.mbdir = kDbDir;
    // Keep the test queue with the rest of the test artifacts so cleanup does
    // not leave a stale _mabain_q file in /dev/shm.
    config.queue_dir = kDbDir;
    config.options = options | CONSTS::OPTION_JEMALLOC;
    config.block_size_index = kBlockSize;
    config.block_size_data = kBlockSize;
    config.max_num_index_block = kMaxBlocks;
    config.max_num_data_block = kMaxBlocks;
    config.memcap_index = static_cast<size_t>(kBlockSize) * kMaxBlocks;
    config.memcap_data = static_cast<size_t>(kBlockSize) * kMaxBlocks;
    config.num_entry_per_bucket = 500;
    return config;
}

class TestDbDirectory {
public:
    TestDbDirectory()
    {
        Reset();
    }

    ~TestDbDirectory()
    {
        namespace fs = std::filesystem;
        std::error_code ec;
        fs::remove_all(kDbDir, ec);
    }

    bool HasContainedQueueFile() const
    {
        namespace fs = std::filesystem;
        std::error_code ec;
        for (const fs::directory_entry& entry : fs::directory_iterator(kDbDir, ec)) {
            if (ec)
                return false;
            const std::string name = entry.path().filename().string();
            if (name.compare(0, sizeof("_mabain_q") - 1, "_mabain_q") == 0)
                return true;
        }
        return false;
    }

private:
    void Reset()
    {
        namespace fs = std::filesystem;
        std::error_code ec;
        fs::remove_all(kDbDir, ec);
        if (ec) {
            std::cerr << "failed to remove test directory: " << ec.message() << '\n';
            std::exit(2);
        }
        fs::create_directories(kDbDir, ec);
        if (ec) {
            std::cerr << "failed to prepare test directory: " << ec.message() << '\n';
            std::exit(2);
        }
    }
};

bool IsCompletePattern(const MBData& data)
{
    if (data.data_len != static_cast<int>(kValueSize) || data.buff == nullptr)
        return false;

    const uint8_t expected = data.buff[0];
    if (expected != static_cast<uint8_t>('A')
        && expected != static_cast<uint8_t>('B'))
        return false;

    for (size_t i = 1; i < kValueSize; ++i) {
        if (data.buff[i] != expected)
            return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv)
{
    const int iterations = argc > 1 ? std::atoi(argv[1]) : 100000;
    const int reader_count = argc > 2 ? std::atoi(argv[2]) : 8;
    if (iterations <= 0 || reader_count <= 0) {
        std::cerr << "usage: " << argv[0] << " [iterations] [readers]\n";
        return 2;
    }

    // Declared before DB handles so its destructor removes the directory only
    // after all mappings and file descriptors have been closed.
    TestDbDirectory test_directory;
    MBConfig writer_config = MakeConfig(CONSTS::WriterOptions());
    DB writer(writer_config);
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << '\n';
        return 2;
    }
    if (!test_directory.HasContainedQueueFile()) {
        std::cerr << "queue file was not created inside the test directory\n";
        return 2;
    }

    const std::string value_a(kValueSize, 'A');
    const std::string value_b(kValueSize, 'B');
    if (writer.Add(kKey, value_a, true) != MBError::SUCCESS) {
        std::cerr << "initial add failed\n";
        return 2;
    }

    std::atomic<bool> start { false };
    std::atomic<bool> stop { false };
    std::atomic<bool> inconsistent { false };
    std::atomic<int> ready { 0 };
    std::atomic<uint64_t> successful_lookups { 0 };
    std::vector<std::thread> readers;
    readers.reserve(static_cast<size_t>(reader_count));

    for (int i = 0; i < reader_count; ++i) {
        readers.emplace_back([&]() {
            MBConfig reader_config = MakeConfig(CONSTS::ReaderOptions());
            DB reader(reader_config);
            if (!reader.is_open()) {
                inconsistent.store(true, std::memory_order_relaxed);
                ready.fetch_add(1, std::memory_order_release);
                return;
            }

            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            MBData data;
            while (!stop.load(std::memory_order_relaxed)
                && !inconsistent.load(std::memory_order_relaxed)) {
                const int rc = reader.Find(kKey, data);
                if (rc == MBError::TRY_AGAIN)
                    continue;
                if (rc != MBError::SUCCESS || !IsCompletePattern(data)) {
                    if (!inconsistent.exchange(true, std::memory_order_relaxed)) {
                        std::cerr << "inconsistent lookup: rc=" << rc
                                  << " length=" << data.data_len << '\n';
                    }
                    break;
                }
                successful_lookups.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != reader_count)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);

    for (int i = 0; i < iterations
         && !inconsistent.load(std::memory_order_relaxed); ++i) {
        const std::string& value = (i & 1) == 0 ? value_b : value_a;
        const int rc = writer.Add(kKey, value, true);
        if (rc != MBError::SUCCESS) {
            std::cerr << "overwrite failed: rc=" << rc << '\n';
            inconsistent.store(true, std::memory_order_relaxed);
            break;
        }
    }

    stop.store(true, std::memory_order_relaxed);
    for (std::thread& reader : readers)
        reader.join();

    if (inconsistent.load(std::memory_order_relaxed))
        return 1;
    if (successful_lookups.load(std::memory_order_relaxed) == 0) {
        std::cerr << "no successful concurrent lookups\n";
        return 2;
    }

    std::cout << "value overwrite concurrency test passed: "
              << successful_lookups.load(std::memory_order_relaxed)
              << " consistent lookups\n";
    return 0;
}
