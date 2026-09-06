/**
 * Validate HashMap value ownership and one-writer/multiple-reader concurrency.
 */

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../error.h"
#include "../hash_map.h"
#include "../mabain_consts.h"
#include "../mb_data.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr size_t kCapacity = 2048;
constexpr size_t kKeyCount = 512;
constexpr size_t kIndexMemcapMb = 1;
constexpr unsigned kReaderProcesses = 4;
constexpr unsigned kThreadsPerProcess = 8;

struct SharedControl {
    std::atomic<uint32_t> ready_threads;
    std::atomic<uint32_t> start;
    std::atomic<uint32_t> allow_miss;
    std::atomic<uint32_t> stop;
    std::atomic<uint64_t> hits;
    std::atomic<uint64_t> retries;
    std::atomic<uint64_t> errors;
    std::atomic<uint64_t> unexpected_misses;
    std::atomic<uint64_t> wrong_values;
    std::atomic<uint64_t> other_errors;
    std::atomic<uint64_t> exhausted_retries;
};

struct Cleanup {
    explicit Cleanup(std::string base)
        : base(std::move(base))
    {
        Remove();
    }

    ~Cleanup() { Remove(); }

    void Remove()
    {
        ResourcePool::getInstance().RemoveAll();
        std::filesystem::path path(base);
        const std::filesystem::path directory = path.parent_path();
        const std::string prefix = path.filename().string() + "_hashmap";
        std::error_code error;
        if (!std::filesystem::exists(directory, error))
            return;
        for (const auto& entry : std::filesystem::directory_iterator(
                 directory, error)) {
            if (error)
                break;
            const std::string name = entry.path().filename().string();
            if (name.compare(0, prefix.size(), prefix) == 0)
                std::filesystem::remove(entry.path(), error);
        }
    }

    std::string base;
};

HashMapValueConfig TestConfig()
{
    HashMapValueConfig config;
    config.value_block_size = 16ULL * 1024ULL * 1024ULL;
    config.value_memcap = 64ULL * 1024ULL * 1024ULL;
    config.reader_slots = 64;
    config.reclaim_threshold_bytes = 128ULL * 1024ULL;
    config.max_retired_bytes = 8ULL * 1024ULL * 1024ULL;
    config.max_load_percent = 85;
    return config;
}

std::string Key(size_t key_id)
{
    std::string key(7, '\0');
    key[0] = static_cast<char>(0xA5);
    key[1] = static_cast<char>(key_id & 0xffU);
    key[2] = static_cast<char>((key_id >> 8) & 0xffU);
    key[3] = '\0';
    key[4] = static_cast<char>((key_id >> 16) & 0xffU);
    key[5] = static_cast<char>((key_id >> 24) & 0xffU);
    key[6] = static_cast<char>(0x5A);
    return key;
}

void Store64(std::string& value, size_t offset, uint64_t number)
{
    for (size_t byte = 0; byte < sizeof(number); ++byte) {
        value[offset + byte]
            = static_cast<char>((number >> (byte * 8)) & 0xffU);
    }
}

uint64_t Load64(const uint8_t* value)
{
    uint64_t number = 0;
    for (size_t byte = 0; byte < sizeof(number); ++byte)
        number |= static_cast<uint64_t>(value[byte]) << (byte * 8);
    return number;
}

std::string Value(size_t key_id, size_t epoch)
{
    const size_t length = 24 + ((key_id * 13 + epoch * 7) % 89);
    std::string value(length, '\0');
    Store64(value, 0, key_id);
    Store64(value, 8, epoch);
    Store64(value, 16, length);
    for (size_t index = 24; index < length; ++index) {
        value[index] = static_cast<char>(
            (key_id * 31 + epoch * 17 + index * 7) & 0xffU);
    }
    return value;
}

bool ValidateValue(size_t expected_key, const MBData& data)
{
    if (data.buff == nullptr || data.data_len < 24)
        return false;
    const uint64_t key_id = Load64(data.buff);
    const uint64_t epoch = Load64(data.buff + 8);
    const uint64_t length = Load64(data.buff + 16);
    if (key_id != expected_key || length != static_cast<uint64_t>(data.data_len))
        return false;
    for (size_t index = 24; index < length; ++index) {
        const uint8_t expected = static_cast<uint8_t>(
            (key_id * 31 + epoch * 17 + index * 7) & 0xffU);
        if (data.buff[index] != expected)
            return false;
    }
    return true;
}

int Put(HashMap& map, size_t key_id, size_t epoch)
{
    const std::string key = Key(key_id);
    const std::string value = Value(key_id, epoch);
    return map.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
        static_cast<int>(key.size()),
        reinterpret_cast<const uint8_t*>(value.data()),
        static_cast<int>(value.size()), true);
}

bool Populate(HashMap& map, size_t epoch)
{
    for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
        if (Put(map, key_id, epoch) != MBError::SUCCESS)
            return false;
    }
    return true;
}

bool OwnerOnly(const std::string& path)
{
    struct stat info {};
    return stat(path.c_str(), &info) == 0
        && (info.st_mode & 0777) == 0600;
}

bool BasicFailure(const char* step)
{
    std::cerr << "basic value-mode failure at: " << step << "\n";
    return false;
}

bool TestBasicValueMode()
{
    const std::string base
        = "/var/tmp/mabain_hashmap_value_basic_" + std::to_string(getpid());
    Cleanup cleanup(base);
    const HashMapValueConfig config = TestConfig();

    try {
        try {
            HashMap missing(base, kCapacity, CONSTS::ACCESS_MODE_READER,
                config, kIndexMemcapMb);
            return BasicFailure("missing reader unexpectedly opened");
        } catch (int error) {
            if (error != MBError::NO_DB)
                return BasicFailure("missing reader returned wrong error");
        }

        HashMap writer(base, kCapacity, CONSTS::ACCESS_MODE_WRITER, config,
            kIndexMemcapMb);
        const std::string key("a\0binary-key", 12);
        const std::string first("\0first\xffvalue", 12);
        const std::string second("second\0value\xfe", 13);
        if (writer.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()),
                reinterpret_cast<const uint8_t*>(first.data()),
                static_cast<int>(first.size()))
            != MBError::SUCCESS) {
            return BasicFailure("initial PutValue");
        }

        MBData data;
        if (writer.GetValue(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()), data)
                != MBError::SUCCESS
            || data.data_len != static_cast<int>(first.size())
            || std::memcmp(data.buff, first.data(), first.size()) != 0) {
            return BasicFailure("initial GetValue bytes");
        }
        if (writer.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()),
                reinterpret_cast<const uint8_t*>(second.data()),
                static_cast<int>(second.size()))
                != MBError::SUCCESS
            || writer.GetValue(reinterpret_cast<const uint8_t*>(key.data()),
                   static_cast<int>(key.size()), data)
                != MBError::SUCCESS
            || data.data_len != static_cast<int>(second.size())
            || std::memcmp(data.buff, second.data(), second.size()) != 0) {
            return BasicFailure("overwrite GetValue bytes");
        }
        if (writer.Erase(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()))
                != MBError::SUCCESS
            || writer.GetValue(reinterpret_cast<const uint8_t*>(key.data()),
                   static_cast<int>(key.size()), data)
                != MBError::NOT_EXIST
            || data.data_len != 0) {
            return BasicFailure("erase lookup contract");
        }
        if (writer.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()),
                reinterpret_cast<const uint8_t*>(first.data()),
                static_cast<int>(first.size()))
            != MBError::SUCCESS) {
            return BasicFailure("reinsert");
        }
        if (writer.PutValue(nullptr, 1,
                reinterpret_cast<const uint8_t*>(first.data()),
                static_cast<int>(first.size()))
                != MBError::INVALID_ARG
            || writer.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
                   static_cast<int>(key.size()), nullptr, 1)
                != MBError::INVALID_ARG) {
            return BasicFailure("invalid input validation");
        }
        if (!OwnerOnly(base + "_hashmap0")
            || !OwnerOnly(base + "_hashmap.lock")
            || !OwnerOnly(base + "_hashmap_values_g1_0")) {
            return BasicFailure("owner-only file permissions");
        }

        pid_t second_writer = fork();
        if (second_writer == 0) {
            try {
                HashMap duplicate(base, kCapacity,
                    CONSTS::ACCESS_MODE_WRITER, config, kIndexMemcapMb);
            } catch (int error) {
                _exit(error == MBError::WRITER_EXIST ? 0 : 1);
            } catch (...) {
                _exit(1);
            }
            _exit(1);
        }
        int writer_status = 0;
        if (second_writer < 0
            || waitpid(second_writer, &writer_status, 0) != second_writer
            || !WIFEXITED(writer_status) || WEXITSTATUS(writer_status) != 0) {
            return BasicFailure("second writer was not rejected");
        }
    } catch (int error) {
        std::cerr << "basic constructor/runtime exception: "
                  << MBError::get_error_str(error) << " (" << error << ")\n";
        return false;
    } catch (...) {
        return BasicFailure("unknown exception");
    }
    return true;
}

int ReadAndValidate(HashMap& reader, size_t key_id, bool allow_miss,
    uint64_t& retries, uint64_t& unexpected_misses,
    uint64_t& wrong_values, uint64_t& other_errors,
    uint64_t& exhausted_retries)
{
    const std::string key = Key(key_id);
    MBData data(128, 0);
    for (unsigned attempt = 0; attempt < 64; ++attempt) {
        int result = reader.GetValue(
            reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()), data);
        if (result == MBError::TRY_AGAIN) {
            ++retries;
            continue;
        }
        if (result == MBError::NOT_EXIST) {
            if (!allow_miss)
                ++unexpected_misses;
            return allow_miss ? 0 : 1;
        }
        if (result != MBError::SUCCESS) {
            ++other_errors;
            return 1;
        }
        if (!ValidateValue(key_id, data)) {
            ++wrong_values;
            return 1;
        }
        return 0;
    }
    ++exhausted_retries;
    // TRY_AGAIN is an allowed bounded-retry result during epoch/generation
    // churn. Report it separately, but do not classify it as bad data.
    return 0;
}

[[noreturn]] void ReaderProcess(const std::string& base,
    const HashMapValueConfig& config, SharedControl* control,
    unsigned process_index)
{
    ResourcePool::getInstance().RemoveAll();
    uint64_t local_hits = 0;
    uint64_t local_retries = 0;
    uint64_t local_errors = 0;
    uint64_t local_unexpected_misses = 0;
    uint64_t local_wrong_values = 0;
    uint64_t local_other_errors = 0;
    uint64_t local_exhausted_retries = 0;
    try {
        HashMap reader(base, kCapacity, CONSTS::ACCESS_MODE_READER, config,
            kIndexMemcapMb);
        std::vector<std::thread> threads;
        threads.reserve(kThreadsPerProcess);
        for (unsigned thread_index = 0; thread_index < kThreadsPerProcess;
             ++thread_index) {
            threads.emplace_back([&, thread_index]() {
                control->ready_threads.fetch_add(1,
                    std::memory_order_release);
                while (control->start.load(std::memory_order_acquire) == 0)
                    std::this_thread::yield();
                uint64_t random = 0x9E3779B97F4A7C15ULL
                    ^ (static_cast<uint64_t>(process_index) << 32)
                    ^ thread_index;
                uint64_t thread_hits = 0;
                uint64_t thread_retries = 0;
                uint64_t thread_errors = 0;
                uint64_t thread_unexpected_misses = 0;
                uint64_t thread_wrong_values = 0;
                uint64_t thread_other_errors = 0;
                uint64_t thread_exhausted_retries = 0;
                while (control->stop.load(std::memory_order_acquire) == 0) {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    const size_t key_id = random % kKeyCount;
                    thread_errors += ReadAndValidate(reader, key_id,
                        control->allow_miss.load(std::memory_order_acquire) != 0,
                        thread_retries, thread_unexpected_misses,
                        thread_wrong_values, thread_other_errors,
                        thread_exhausted_retries);
                    ++thread_hits;
                }
                __atomic_fetch_add(&local_hits, thread_hits, __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_retries, thread_retries,
                    __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_errors, thread_errors,
                    __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_unexpected_misses,
                    thread_unexpected_misses, __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_wrong_values,
                    thread_wrong_values, __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_other_errors,
                    thread_other_errors, __ATOMIC_RELAXED);
                __atomic_fetch_add(&local_exhausted_retries,
                    thread_exhausted_retries, __ATOMIC_RELAXED);
            });
        }
        for (std::thread& thread : threads)
            thread.join();
    } catch (...) {
        ++local_errors;
        control->ready_threads.fetch_add(kThreadsPerProcess,
            std::memory_order_release);
    }

    control->hits.fetch_add(local_hits, std::memory_order_relaxed);
    control->retries.fetch_add(local_retries, std::memory_order_relaxed);
    control->errors.fetch_add(local_errors, std::memory_order_relaxed);
    control->unexpected_misses.fetch_add(local_unexpected_misses,
        std::memory_order_relaxed);
    control->wrong_values.fetch_add(local_wrong_values,
        std::memory_order_relaxed);
    control->other_errors.fetch_add(local_other_errors,
        std::memory_order_relaxed);
    control->exhausted_retries.fetch_add(local_exhausted_retries,
        std::memory_order_relaxed);
    _exit(local_errors == 0 ? 0 : 1);
}

bool WaitForReaders(SharedControl* control)
{
    const uint32_t expected = kReaderProcesses * kThreadsPerProcess;
    for (unsigned wait = 0; wait < 20000; ++wait) {
        if (control->ready_threads.load(std::memory_order_acquire) == expected)
            return true;
        usleep(1000);
    }
    return false;
}

bool TestConcurrentValues()
{
    const std::string base = "/var/tmp/mabain_hashmap_value_concurrency_"
        + std::to_string(getpid());
    Cleanup cleanup(base);
    const HashMapValueConfig config = TestConfig();
    SharedControl* control = static_cast<SharedControl*>(mmap(nullptr,
        sizeof(SharedControl), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (control == MAP_FAILED)
        return false;
    new (&control->ready_threads) std::atomic<uint32_t>(0);
    new (&control->start) std::atomic<uint32_t>(0);
    new (&control->allow_miss) std::atomic<uint32_t>(0);
    new (&control->stop) std::atomic<uint32_t>(0);
    new (&control->hits) std::atomic<uint64_t>(0);
    new (&control->retries) std::atomic<uint64_t>(0);
    new (&control->errors) std::atomic<uint64_t>(0);
    new (&control->unexpected_misses) std::atomic<uint64_t>(0);
    new (&control->wrong_values) std::atomic<uint64_t>(0);
    new (&control->other_errors) std::atomic<uint64_t>(0);
    new (&control->exhausted_retries) std::atomic<uint64_t>(0);

    bool success = true;
    std::vector<pid_t> children;
    try {
        std::unique_ptr<HashMap> writer(new HashMap(base, kCapacity,
            CONSTS::ACCESS_MODE_WRITER, config, kIndexMemcapMb));
        success = Populate(*writer, 0);
        for (unsigned process = 0;
             success && process < kReaderProcesses; ++process) {
            pid_t child = fork();
            if (child == 0)
                ReaderProcess(base, config, control, process);
            if (child < 0) {
                success = false;
                break;
            }
            children.push_back(child);
        }
        if (success)
            success = WaitForReaders(control);
        control->start.store(1, std::memory_order_release);

        for (size_t epoch = 1; success && epoch <= 100; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                if (Put(*writer, key_id, epoch) != MBError::SUCCESS) {
                    success = false;
                    break;
                }
            }
        }

        control->allow_miss.store(1, std::memory_order_release);
        for (size_t epoch = 101; success && epoch <= 150; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                const std::string key = Key(key_id);
                if ((key_id + epoch) % 17 == 0
                    && writer->Erase(
                           reinterpret_cast<const uint8_t*>(key.data()),
                           static_cast<int>(key.size()))
                        != MBError::SUCCESS) {
                    success = false;
                    break;
                }
                if (Put(*writer, key_id, epoch) != MBError::SUCCESS) {
                    success = false;
                    break;
                }
            }
        }

        writer.reset();
        if (success) {
            writer.reset(new HashMap(base, kCapacity,
                CONSTS::ACCESS_MODE_WRITER, config, kIndexMemcapMb));
            success = Populate(*writer, 151);
        }
        for (size_t epoch = 152; success && epoch <= 200; ++epoch) {
            for (size_t key_id = 0; key_id < kKeyCount; ++key_id) {
                if (Put(*writer, key_id, epoch) != MBError::SUCCESS) {
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
    if (children.size() != kReaderProcesses
        || control->hits.load(std::memory_order_relaxed) == 0
        || control->errors.load(std::memory_order_relaxed) != 0) {
        success = false;
    }

    std::cout << "value readers="
              << kReaderProcesses * kThreadsPerProcess
              << " hits=" << control->hits.load(std::memory_order_relaxed)
              << " retries="
              << control->retries.load(std::memory_order_relaxed)
              << " errors="
              << control->errors.load(std::memory_order_relaxed)
              << " unexpected_misses="
              << control->unexpected_misses.load(std::memory_order_relaxed)
              << " wrong_values="
              << control->wrong_values.load(std::memory_order_relaxed)
              << " other_errors="
              << control->other_errors.load(std::memory_order_relaxed)
              << " exhausted_retries="
              << control->exhausted_retries.load(std::memory_order_relaxed)
              << "\n";
    munmap(control, sizeof(SharedControl));
    return success;
}

} // namespace

int main()
{
    if (!TestBasicValueMode()) {
        std::cerr << "HashMap basic value-mode test failed\n";
        return 1;
    }
    if (!TestConcurrentValues()) {
        std::cerr << "HashMap concurrent value-mode test failed\n";
        return 2;
    }
    std::cout << "HashMap value-mode validation passed\n";
    return 0;
}
