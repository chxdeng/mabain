/**
 * Validate HashMap's one-writer/multiple-reader process contract.
 */

#include <atomic>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "../error.h"
#include "../hash_map_api.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr size_t kCapacity = 4096;
constexpr size_t kKeyCount = 1024;
constexpr size_t kMemcapMb = 1;
constexpr unsigned kReaderCount = 4;

struct SharedControl {
    std::atomic<uint32_t> ready;
    std::atomic<uint32_t> start;
    std::atomic<uint32_t> allow_miss;
    std::atomic<uint32_t> stop;
    std::atomic<uint64_t> errors;
    std::atomic<uint64_t> hits;
};

struct FileCleanup {
    explicit FileCleanup(std::string base)
        : backing_file(std::move(base) + "_hashmap0")
    {
    }

    ~FileCleanup()
    {
        unlink(backing_file.c_str());
    }

    std::string backing_file;
};

std::string Key(size_t value)
{
    std::string key(4, '\0');
    key[0] = static_cast<char>(value & 0xffU);
    key[1] = static_cast<char>((value >> 8) & 0xffU);
    key[2] = static_cast<char>((value >> 16) & 0xffU);
    key[3] = static_cast<char>((value >> 24) & 0xffU);
    return key;
}

int PutValue(HashMap& map, size_t key_id, size_t epoch)
{
    const std::string key = Key(key_id);
    return map.Put(reinterpret_cast<const uint8_t*>(key.data()),
        static_cast<int>(key.size()), epoch * kKeyCount + key_id, true);
}

bool Populate(HashMap& map, size_t epoch)
{
    for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
        if (PutValue(map, key_id, epoch) != MBError::SUCCESS)
            return false;
    }
    return true;
}

bool TestReaderDoesNotCreate()
{
    const std::string base = "/var/tmp/mabain_hashmap_missing_"
        + std::to_string(getpid());
    FileCleanup cleanup(base);
    try {
        HashMap reader(base, kCapacity, CONSTS::ACCESS_MODE_READER, 1, 24,
            kMemcapMb, true);
    } catch (int error) {
        return error == MBError::NO_DB;
    }
    return false;
}

bool TestProbeChain(bool compact)
{
    const std::string base = "/var/tmp/mabain_hashmap_probe_"
        + std::to_string(getpid()) + (compact ? "_compact" : "_full");
    FileCleanup cleanup(base);
    bool success = true;
    try {
        HashMap writer(base, kCapacity, CONSTS::ACCESS_MODE_WRITER, 1, 24,
            kMemcapMb, compact);
        for (size_t key_id = 0; key_id < 3000; ++key_id) {
            const std::string key = Key(key_id);
            if (writer.Put(reinterpret_cast<const uint8_t*>(key.data()),
                    static_cast<int>(key.size()), key_id, true)
                != MBError::SUCCESS) {
                success = false;
                break;
            }
        }
        for (size_t key_id = 0; success && key_id < 3000; key_id += 3) {
            const std::string key = Key(key_id);
            if (writer.Erase(reinterpret_cast<const uint8_t*>(key.data()),
                    static_cast<int>(key.size()))
                != MBError::SUCCESS) {
                success = false;
            }
        }
        for (size_t key_id = 0; success && key_id < 3000; ++key_id) {
            const std::string key = Key(key_id);
            size_t ref = 0;
            const bool found = writer.Get(
                reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()), ref);
            const bool erased = key_id % 3 == 0;
            if (found == erased || (found && ref != key_id))
                success = false;
        }
    } catch (...) {
        success = false;
    }
    ResourcePool::getInstance().RemoveAll();
    return success;
}

[[noreturn]] void ReaderProcess(const std::string& base, bool compact,
    SharedControl* control, unsigned reader_id)
{
    uint64_t local_errors = 0;
    uint64_t local_hits = 0;
    ResourcePool::getInstance().RemoveAll();
    try {
        HashMap reader(base, kCapacity, CONSTS::ACCESS_MODE_READER, 1, 24,
            kMemcapMb, compact);
        control->ready.fetch_add(1, std::memory_order_release);
        while (control->start.load(std::memory_order_acquire) == 0)
            usleep(100);

        uint64_t random = 0x9E3779B97F4A7C15ULL + reader_id;
        while (control->stop.load(std::memory_order_acquire) == 0) {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            const size_t key_id = static_cast<size_t>(random) % kKeyCount;
            const std::string key = Key(key_id);
            size_t ref = 0;
            bool found = reader.Get(
                reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()), ref);
            if (found) {
                ++local_hits;
                if (ref % kKeyCount != key_id)
                    ++local_errors;
            } else if (control->allow_miss.load(std::memory_order_acquire) == 0) {
                ++local_errors;
            }
        }
    } catch (...) {
        ++local_errors;
        control->ready.fetch_add(1, std::memory_order_release);
    }

    control->hits.fetch_add(local_hits, std::memory_order_relaxed);
    control->errors.fetch_add(local_errors, std::memory_order_relaxed);
    _exit(local_errors == 0 ? 0 : 1);
}

bool WaitForReaders(SharedControl* control)
{
    for (unsigned wait = 0; wait < 10000; ++wait) {
        if (control->ready.load(std::memory_order_acquire) == kReaderCount)
            return true;
        usleep(1000);
    }
    return false;
}

bool TestConcurrentReaders(bool compact)
{
    const std::string base = "/var/tmp/mabain_hashmap_process_"
        + std::to_string(getpid()) + (compact ? "_compact" : "_full");
    FileCleanup cleanup(base);
    SharedControl* control = reinterpret_cast<SharedControl*>(mmap(nullptr,
        sizeof(SharedControl), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (control == MAP_FAILED)
        return false;
    new (&control->ready) std::atomic<uint32_t>(0);
    new (&control->start) std::atomic<uint32_t>(0);
    new (&control->allow_miss) std::atomic<uint32_t>(0);
    new (&control->stop) std::atomic<uint32_t>(0);
    new (&control->errors) std::atomic<uint64_t>(0);
    new (&control->hits) std::atomic<uint64_t>(0);

    bool success = true;
    std::vector<pid_t> children;
    try {
        std::unique_ptr<HashMap> writer(new HashMap(base, kCapacity,
            CONSTS::ACCESS_MODE_WRITER, 1, 24, kMemcapMb, compact));
        success = Populate(*writer, 0);

        for (unsigned reader = 0; success && reader < kReaderCount; ++reader) {
            pid_t pid = fork();
            if (pid == 0)
                ReaderProcess(base, compact, control, reader);
            if (pid < 0) {
                success = false;
                break;
            }
            children.push_back(pid);
        }

        if (success)
            success = WaitForReaders(control);
        control->start.store(1, std::memory_order_release);

        // Overwrite-only phase: every key remains present, so readers must not
        // report even temporary misses.
        for (size_t epoch = 1; success && epoch <= 300; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                if (PutValue(*writer, key_id, epoch) != MBError::SUCCESS) {
                    success = false;
                    break;
                }
            }
        }

        // Remove/reinsert and writer restart may overlap lookups. A miss is
        // valid, but a returned offset must always belong to the queried key.
        control->allow_miss.store(1, std::memory_order_release);
        for (size_t epoch = 301; success && epoch <= 450; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                const std::string key = Key(key_id);
                if ((key_id + epoch) % 17 == 0) {
                    if (writer->Erase(
                            reinterpret_cast<const uint8_t*>(key.data()),
                            static_cast<int>(key.size()))
                        != MBError::SUCCESS) {
                        success = false;
                        break;
                    }
                }
                if (PutValue(*writer, key_id, epoch) != MBError::SUCCESS) {
                    success = false;
                    break;
                }
            }
        }

        writer.reset();
        if (success) {
            writer.reset(new HashMap(base, kCapacity,
                CONSTS::ACCESS_MODE_WRITER, 1, 24, kMemcapMb, compact));
            success = Populate(*writer, 451);
        }
        for (size_t epoch = 452; success && epoch <= 550; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                if (PutValue(*writer, key_id, epoch) != MBError::SUCCESS) {
                    success = false;
                    break;
                }
            }
        }
        writer.reset();
    } catch (...) {
        success = false;
    }

    control->stop.store(1, std::memory_order_release);
    control->start.store(1, std::memory_order_release);
    for (pid_t child : children) {
        int status = 0;
        if (waitpid(child, &status, 0) != child || !WIFEXITED(status)
            || WEXITSTATUS(status) != 0) {
            success = false;
        }
    }
    if (children.size() != kReaderCount
        || control->errors.load(std::memory_order_relaxed) != 0
        || control->hits.load(std::memory_order_relaxed) == 0) {
        success = false;
    }

    std::cout << (compact ? "compact" : "full")
              << " multiprocess reader hits="
              << control->hits.load(std::memory_order_relaxed)
              << " errors=" << control->errors.load(std::memory_order_relaxed)
              << "\n";
    munmap(control, sizeof(SharedControl));
    ResourcePool::getInstance().RemoveAll();
    return success;
}

} // namespace

int main()
{
    if (!std::atomic<uint64_t>().is_lock_free()) {
        std::cerr << "64-bit atomics are not lock-free\n";
        return 1;
    }
    if (!TestReaderDoesNotCreate()) {
        std::cerr << "reader creation guard failed\n";
        return 2;
    }
    if (!TestProbeChain(true) || !TestProbeChain(false)) {
        std::cerr << "tombstone probe-chain test failed\n";
        return 3;
    }
    if (!TestConcurrentReaders(true) || !TestConcurrentReaders(false)) {
        std::cerr << "multiprocess concurrency test failed\n";
        return 4;
    }
    std::cout << "HashMap multiprocess concurrency tests passed\n";
    return 0;
}
