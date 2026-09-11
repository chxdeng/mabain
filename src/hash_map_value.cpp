/**
 * HashMap-owned immutable key/value records and reader-safe reclamation.
 */

#include "hash_map_internal.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <limits>
#include <new>
#include <sstream>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <signal.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "error.h"
#include "mb_data.h"
#include "resource_pool.h"

namespace mabain {

namespace {

constexpr uint32_t kHashMapMagic = 0x484D4150U; // 'HMAP'
constexpr uint16_t kValueLayoutVersion = 4;
constexpr uint64_t kValueControl = static_cast<uint64_t>(kHashMapMagic)
    | (static_cast<uint64_t>(kValueLayoutVersion) << 32);
constexpr uint64_t kEmptyValueEntry = 0;
constexpr uint64_t kTombstoneValueEntry = 1;
constexpr unsigned kValueOffsetBits = 48;
constexpr uint64_t kValueOffsetLimit = uint64_t { 1 } << kValueOffsetBits;
constexpr uint64_t kValueOffsetMask = kValueOffsetLimit - 1;
constexpr uint64_t kSlotClaiming = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kFirstValueGeneration = 1;
constexpr uint64_t kFirstReaderEpoch = 1;
constexpr size_t kMinimumCapacity = 1024;
constexpr size_t kValueReservedOffset = 4096;
constexpr size_t kMinimumValueBlockSize = 4ULL * 1024ULL * 1024ULL;
constexpr unsigned kLookupRetries = 8;
constexpr uint32_t kMaxValueReaderSlots = 128;
constexpr mode_t kOwnerFileMode = S_IRUSR | S_IWUSR;

uint64_t PackValueEntry(uint64_t hash, size_t record_offset)
{
    return (hash & ~kValueOffsetMask)
        | static_cast<uint64_t>(record_offset);
}

size_t ValueEntryOffset(uint64_t entry)
{
    return static_cast<size_t>(entry & kValueOffsetMask);
}

bool ValueEntryHashMatches(uint64_t entry, uint64_t hash)
{
    return (entry & ~kValueOffsetMask) == (hash & ~kValueOffsetMask);
}

#ifdef MB_HAVE_XXHASH
constexpr uint32_t kHashAlgorithm = 2;
#else
constexpr uint32_t kHashAlgorithm = 1;
#endif

struct ValueRecordHeader {
    uint32_t value_length;
    uint16_t key_length;
    uint16_t flags;
};

static_assert(sizeof(ValueRecordHeader) == 8,
    "HashMap value record header layout changed");

struct ThreadSlotEntry {
    uint64_t connection_id;
    size_t slot_index;
    uint64_t owner_id;
    std::shared_ptr<struct ConnectionToken> connection;
};

struct ClaimedSlot {
    size_t slot_index;
    uint64_t owner_id;
};

struct ConnectionToken {
    std::atomic<bool> active { true };
    void* context = nullptr;
    void (*release_slot)(void*, size_t, uint64_t) = nullptr;
};

class ThreadSlotCache {
public:
    ~ThreadSlotCache()
    {
        for (const ThreadSlotEntry& entry : entries_) {
            if (entry.connection != nullptr
                && entry.connection->active.load(std::memory_order_acquire)
                && entry.connection->release_slot != nullptr) {
                entry.connection->release_slot(entry.connection->context,
                    entry.slot_index, entry.owner_id);
            }
        }
    }

    ThreadSlotEntry* Find(uint64_t connection_id)
    {
        for (ThreadSlotEntry& entry : entries_) {
            if (entry.connection_id == connection_id)
                return &entry;
        }
        return nullptr;
    }

    ThreadSlotEntry* Add(const ThreadSlotEntry& entry)
    {
        entries_.push_back(entry);
        return &entries_.back();
    }

private:
    std::vector<ThreadSlotEntry> entries_;
};

thread_local ThreadSlotCache g_thread_slots;

std::atomic<uint64_t>& ConnectionCounter()
{
    static std::atomic<uint64_t> counter { 1 };
    return counter;
}

std::atomic<uint64_t>& OwnerCounter()
{
    static std::atomic<uint64_t> counter { 1 };
    return counter;
}

uint64_t Mix64(uint64_t value)
{
    value += 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31);
}

bool ReadProcessStartTime(pid_t pid, uint64_t& start_time)
{
    std::ifstream input("/proc/" + std::to_string(pid) + "/stat");
    std::string line;
    if (!input.is_open() || !std::getline(input, line))
        return false;

    const size_t close_paren = line.rfind(')');
    if (close_paren == std::string::npos || close_paren + 2 >= line.size())
        return false;

    std::istringstream fields(line.substr(close_paren + 2));
    std::string ignored;
    // The substring starts at field 3. Skip fields 3 through 21.
    for (unsigned field = 3; field <= 21; ++field) {
        if (!(fields >> ignored))
            return false;
    }
    return static_cast<bool>(fields >> start_time) && start_time != 0;
}

bool ProcessInstanceIsAlive(uint64_t process_id, uint64_t start_time)
{
    if (process_id == 0 || process_id > static_cast<uint64_t>(
            std::numeric_limits<pid_t>::max())
        || start_time == 0) {
        return false;
    }

    const pid_t pid = static_cast<pid_t>(process_id);
    uint64_t observed_start = 0;
    if (ReadProcessStartTime(pid, observed_start))
        return observed_start == start_time;

    // Failure to inspect a process is not proof that it died. Only ESRCH is a
    // safe fallback signal; permission errors retain the slot.
    if (kill(pid, 0) == 0 || errno == EPERM)
        return true;
    return errno != ESRCH;
}

bool NextPowerOfTwo(size_t value, size_t& result)
{
    if (value <= 1) {
        result = 1;
        return true;
    }
    const size_t highest = size_t { 1 }
        << (std::numeric_limits<size_t>::digits - 1);
    if (value > highest)
        return false;
    --value;
    for (size_t shift = 1; shift < std::numeric_limits<size_t>::digits;
         shift <<= 1) {
        value |= value >> shift;
    }
    result = value + 1;
    return true;
}

bool CheckedAdd(size_t left, size_t right, size_t& result)
{
    if (left > std::numeric_limits<size_t>::max() - right)
        return false;
    result = left + right;
    return true;
}

bool RecordSize(uint32_t key_length, uint32_t value_length, size_t& size)
{
    size_t with_key = 0;
    return CheckedAdd(sizeof(ValueRecordHeader), key_length, with_key)
        && CheckedAdd(with_key, value_length, size);
}

int CreateExclusiveSizedFile(const std::string& path, size_t size)
{
    if (size > static_cast<size_t>(std::numeric_limits<off_t>::max()))
        return MBError::INVALID_SIZE;
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
        kOwnerFileMode);
    if (fd < 0)
        return errno == EEXIST ? MBError::IN_DICT : MBError::OPEN_FAILURE;

    int result = MBError::SUCCESS;
    while (ftruncate(fd, static_cast<off_t>(size)) != 0) {
        if (errno == EINTR)
            continue;
        result = MBError::WRITE_ERROR;
        break;
    }
    if (close(fd) != 0 && result == MBError::SUCCESS)
        result = MBError::WRITE_ERROR;
    if (result != MBError::SUCCESS)
        unlink(path.c_str());
    return result;
}

void SplitPath(const std::string& path, std::string& directory,
    std::string& name)
{
    const size_t slash = path.rfind('/');
    if (slash == std::string::npos) {
        directory = ".";
        name = path;
    } else {
        directory = slash == 0 ? "/" : path.substr(0, slash);
        name = path.substr(slash + 1);
    }
}

bool ParseValueFileName(const std::string& name, const std::string& prefix,
    uint64_t& generation)
{
    if (name.compare(0, prefix.size(), prefix) != 0)
        return false;
    const char* begin = name.c_str() + prefix.size();
    if (*begin < '0' || *begin > '9')
        return false;
    errno = 0;
    char* after_generation = nullptr;
    unsigned long long parsed = std::strtoull(begin, &after_generation, 10);
    if (errno != 0 || after_generation == begin || *after_generation != '_')
        return false;
    const char* block = after_generation + 1;
    if (*block < '0' || *block > '9')
        return false;
    char* end = nullptr;
    errno = 0;
    (void)std::strtoull(block, &end, 10);
    if (errno != 0 || end == block || *end != '\0')
        return false;
    generation = static_cast<uint64_t>(parsed);
    return generation != 0;
}

uint64_t HighestValueGeneration(const std::string& map_path)
{
    std::string directory;
    std::string map_name;
    SplitPath(map_path, directory, map_name);
    const std::string prefix = map_name + "_values_g";
    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr)
        return 0;

    uint64_t highest = 0;
    while (dirent* entry = readdir(dir)) {
        uint64_t generation = 0;
        if (ParseValueFileName(entry->d_name, prefix, generation))
            highest = std::max(highest, generation);
    }
    closedir(dir);
    return highest;
}

void RemoveOldValueFiles(const std::string& map_path, uint64_t keep_generation)
{
    std::string directory;
    std::string map_name;
    SplitPath(map_path, directory, map_name);
    const std::string prefix = map_name + "_values_g";
    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr)
        return;

    while (dirent* entry = readdir(dir)) {
        uint64_t generation = 0;
        if (!ParseValueFileName(entry->d_name, prefix, generation)
            || generation == keep_generation) {
            continue;
        }
        const std::string path = directory + "/" + entry->d_name;
        ResourcePool::getInstance().RemoveResourceByPath(path);
        unlink(path.c_str());
    }
    closedir(dir);
}

class ValueGenerationView {
public:
    ValueGenerationView(std::string path_prefix, uint64_t generation,
        size_t block_size, uint32_t max_blocks)
        : path_prefix_(std::move(path_prefix))
        , generation_(generation)
        , block_size_(block_size)
        , max_blocks_(max_blocks)
        , blocks_(new std::atomic<uint8_t*>[max_blocks])
        , retired_(false)
        , next_(nullptr)
    {
        for (uint32_t block = 0; block < max_blocks_; ++block)
            blocks_[block].store(nullptr, std::memory_order_relaxed);
    }

    ~ValueGenerationView()
    {
        RetireMappings();
    }

    uint64_t Generation() const { return generation_; }
    ValueGenerationView* Next() const { return next_; }
    void SetNext(ValueGenerationView* next) { next_ = next; }

    void RetireMappings()
    {
        retired_.store(true, std::memory_order_release);
        for (uint32_t block = 0; block < max_blocks_; ++block) {
            uint8_t* address
                = blocks_[block].exchange(nullptr, std::memory_order_acq_rel);
            if (address != nullptr)
                munmap(address, block_size_);
        }
    }

    uint8_t* Resolve(size_t offset, size_t size)
    {
        if (offset == 0 || size == 0
            || retired_.load(std::memory_order_acquire))
            return nullptr;
        const size_t block = offset / block_size_;
        const size_t relative = offset % block_size_;
        if (block >= max_blocks_ || size > block_size_
            || relative > block_size_ - size) {
            return nullptr;
        }

        uint8_t* address = blocks_[block].load(std::memory_order_acquire);
        if (address == nullptr)
            address = MapBlock(static_cast<uint32_t>(block));
        return address == nullptr ? nullptr : address + relative;
    }

private:
    uint8_t* MapBlock(uint32_t block)
    {
        const std::string path = path_prefix_ + std::to_string(block);
        int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            return nullptr;

        struct stat info {};
        if (fstat(fd, &info) != 0 || info.st_size < 0
            || static_cast<uint64_t>(info.st_size) != block_size_) {
            close(fd);
            return nullptr;
        }
        void* mapped = mmap(nullptr, block_size_, PROT_READ, MAP_SHARED, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            return nullptr;
        }
        uint8_t* address = static_cast<uint8_t*>(mapped);
        close(fd);
        if (retired_.load(std::memory_order_acquire)) {
            munmap(address, block_size_);
            return nullptr;
        }

        uint8_t* expected = nullptr;
        if (!blocks_[block].compare_exchange_strong(expected, address,
                std::memory_order_release, std::memory_order_acquire)) {
            munmap(address, block_size_);
            return expected;
        }
        if (retired_.load(std::memory_order_acquire)) {
            uint8_t* installed
                = blocks_[block].exchange(nullptr, std::memory_order_acq_rel);
            if (installed != nullptr)
                munmap(installed, block_size_);
            return nullptr;
        }
        return address;
    }

    std::string path_prefix_;
    uint64_t generation_;
    size_t block_size_;
    uint32_t max_blocks_;
    std::unique_ptr<std::atomic<uint8_t*>[]> blocks_;
    std::atomic<bool> retired_;
    ValueGenerationView* next_;
};

enum class RecordMatch {
    MATCH,
    DIFFERENT,
    ERROR,
    NO_MEMORY,
};

} // namespace

class HashMapValueState {
public:
    HashMapValueState(HashMapImpl& map, const HashMapValueConfig& config,
        bool writer)
        : map_(map)
        , config_(config)
        , header_(nullptr)
        , writer_(writer)
        , process_id_(static_cast<uint64_t>(getpid()))
        , process_start_time_(0)
        , connection_id_(0)
        , lock_fd_(-1)
        , current_view_(nullptr)
        , views_head_(nullptr)
        , writer_generation_(0)
        , retired_bytes_(0)
    {
        if (!ReadProcessStartTime(static_cast<pid_t>(process_id_),
                process_start_time_)) {
            throw static_cast<int>(MBError::OPEN_FAILURE);
        }
        const uint64_t sequence
            = ConnectionCounter().fetch_add(1, std::memory_order_relaxed);
        connection_id_ = Mix64(process_id_ ^ process_start_time_ ^ sequence);
        if (connection_id_ == 0)
            connection_id_ = sequence == 0 ? 1 : sequence;
        connection_token_ = std::make_shared<ConnectionToken>();
        connection_token_->context = this;
        connection_token_->release_slot
            = &HashMapValueState::ReleaseSlotThunk;
        if (writer_)
            AcquireWriterLock();
    }

    ~HashMapValueState()
    {
        connection_token_->active.store(false, std::memory_order_release);
        ClearOwnedSlots();
        current_view_.store(nullptr, std::memory_order_seq_cst);
        ValueGenerationView* view
            = views_head_.exchange(nullptr, std::memory_order_acq_rel);
        while (view != nullptr) {
            ValueGenerationView* next = view->Next();
            delete view;
            view = next;
        }
        writer_file_.reset();
        if (lock_fd_ >= 0) {
            flock(lock_fd_, LOCK_UN);
            close(lock_fd_);
            lock_fd_ = -1;
        }
    }

    void Attach(HashMapImpl::ValueHMHeader* header)
    {
        header_ = header;
        local_owners_.reset(
            new std::atomic<uint64_t>[header_->reader_slot_count]);
        for (uint32_t slot = 0; slot < header_->reader_slot_count; ++slot)
            local_owners_[slot].store(0, std::memory_order_relaxed);

    }

    uint64_t PrepareWriterGeneration(uint64_t stored_generation)
    {
        if (!writer_)
            throw static_cast<int>(MBError::NOT_ALLOWED);
        uint64_t highest
            = std::max(stored_generation, HighestValueGeneration(map_.path_));
        if (highest == std::numeric_limits<uint64_t>::max())
            throw static_cast<int>(MBError::NO_RESOURCE);
        writer_generation_ = std::max(kFirstValueGeneration, highest + 1);
        const std::string value_path = ValuePath(writer_generation_);
        const int create_result = CreateExclusiveSizedFile(
            value_path + "0", config_.value_block_size);
        if (create_result != MBError::SUCCESS)
            throw static_cast<int>(create_result);

        int value_options
            = CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC;
        if ((map_.options_ & CONSTS::SYNC_ON_WRITE) != 0)
            value_options |= CONSTS::SYNC_ON_WRITE;
        const uint32_t max_blocks = static_cast<uint32_t>(
            config_.value_memcap / config_.value_block_size);
        writer_file_.reset(new RollableFile(value_path,
            config_.value_block_size, config_.value_memcap, value_options,
            max_blocks, 75, kOwnerFileMode));
        if (writer_file_->PreAlloc(kValueReservedOffset) == nullptr)
            throw static_cast<int>(MBError::MMAP_FAILED);
        return writer_generation_;
    }

    void InstallView(uint64_t generation)
    {
        for (;;) {
            ValueGenerationView* current
                = current_view_.load(std::memory_order_seq_cst);
            if (current != nullptr && current->Generation() == generation)
                return;
            if (header_ != nullptr
                && header_->value_generation.load(std::memory_order_acquire)
                    != generation) {
                return;
            }

            const uint32_t max_blocks = header_ != nullptr
                ? header_->max_value_blocks
                : static_cast<uint32_t>(
                      config_.value_memcap / config_.value_block_size);
            ValueGenerationView* next = new ValueGenerationView(
                ValuePath(generation), generation, config_.value_block_size,
                max_blocks);
            if (current_view_.compare_exchange_strong(current, next,
                    std::memory_order_seq_cst,
                    std::memory_order_seq_cst)) {
                ValueGenerationView* head
                    = views_head_.load(std::memory_order_acquire);
                do {
                    next->SetNext(head);
                } while (!views_head_.compare_exchange_weak(head, next,
                    std::memory_order_release, std::memory_order_acquire));
                RetireOldViews();
                return;
            }
            delete next;
        }
    }

    ValueGenerationView* EnsureView(uint64_t generation)
    {
        ValueGenerationView* view
            = current_view_.load(std::memory_order_seq_cst);
        if (view == nullptr || view->Generation() != generation) {
            InstallView(generation);
            view = current_view_.load(std::memory_order_seq_cst);
        }
        return view != nullptr && view->Generation() == generation
            ? view
            : nullptr;
    }

    void CleanupGenerationFiles(uint64_t keep_generation)
    {
        RemoveOldValueFiles(map_.path_, keep_generation);
    }

    int Put(const uint8_t* key, int key_length, const uint8_t* value,
        int value_length, bool overwrite)
    {
        if (!writer_)
            return MBError::NOT_ALLOWED;
        if (!ValidInput(key, key_length, value, value_length))
            return MBError::INVALID_ARG;

        const uint64_t generation
            = header_->value_generation.load(std::memory_order_acquire);
        ValueGenerationView* view = EnsureView(generation);
        if (view == nullptr)
            return MBError::READ_ERROR;

        const uint64_t hash = HashMapImpl::normalize_hash(
            HashMapImpl::fnv1a64(key, key_length));
        ProbeResult probe;
        int result = ProbeForWriter(view, key, key_length, hash, probe);
        if (result != MBError::SUCCESS)
            return result;
        if (probe.found && !overwrite)
            return MBError::SUCCESS;
        if (!probe.found
            && header_->used.load(std::memory_order_relaxed)
                >= MaximumOccupancy()) {
            return MBError::NO_RESOURCE;
        }

        if (probe.found && !CanRetire(probe.old_size))
            return MBError::TRY_AGAIN;

        size_t new_offset = 0;
        size_t new_size = 0;
        void* new_record = AllocateRecord(key, key_length, value, value_length,
            new_offset, new_size);
        if (new_record == nullptr)
            return writer_file_->GetLastAllocError();

        HashMapImpl::ValueBucket* bucket = Bucket(probe.index);
        if (probe.found) {
            const uint64_t retire_epoch
                = header_->reader_epoch.load(std::memory_order_seq_cst);
            try {
                retired_.push_back({ generation, probe.old_offset,
                    probe.old_size, retire_epoch });
            } catch (const std::bad_alloc&) {
                writer_file_->Free(new_record);
                return MBError::NO_MEMORY;
            }
            bucket->entry.store(PackValueEntry(hash, new_offset),
                std::memory_order_release);
            retired_bytes_ += probe.old_size;
            header_->retired_value_bytes.store(retired_bytes_,
                std::memory_order_relaxed);
            header_->live_value_bytes.fetch_sub(probe.old_size,
                std::memory_order_relaxed);
        } else {
            bucket->entry.store(PackValueEntry(hash, new_offset),
                std::memory_order_release);
            header_->used.fetch_add(1, std::memory_order_relaxed);
            if (probe.reused_tombstone)
                header_->tombstones.fetch_sub(1, std::memory_order_relaxed);
        }
        header_->live_value_bytes.fetch_add(new_size,
            std::memory_order_relaxed);
        MaybeReclaim();
        return MBError::SUCCESS;
    }

    int Get(const uint8_t* key, int key_length, MBData& value)
    {
        value.data_len = 0;
        if (key == nullptr || key_length <= 0
            || key_length > CONSTS::MAX_KEY_LENGHTH) {
            return MBError::INVALID_ARG;
        }

        ClaimedSlot slot_entry {};
        int claim_result = GetThreadSlot(slot_entry);
        if (claim_result != MBError::SUCCESS)
            return claim_result;
        HashMapImpl::ValueReaderSlot& slot
            = header_->reader_slots[slot_entry.slot_index];

        for (unsigned attempt = 0; attempt < kLookupRetries; ++attempt) {
            const uint64_t map_generation
                = header_->map_generation.load(std::memory_order_acquire);
            if ((map_generation & 1U) != 0)
                continue;
            const uint64_t value_generation
                = header_->value_generation.load(std::memory_order_acquire);
            if (value_generation == 0)
                continue;
            ValueGenerationView* view = EnsureView(value_generation);
            if (view == nullptr)
                continue;
            const uint64_t epoch
                = header_->reader_epoch.load(std::memory_order_seq_cst);

            ActiveSlotGuard guard(slot);
            guard.Publish(value_generation, epoch);
            if (header_->reader_epoch.load(std::memory_order_seq_cst) != epoch
                || header_->map_generation.load(std::memory_order_acquire)
                    != map_generation
                || header_->value_generation.load(std::memory_order_acquire)
                    != value_generation
                || current_view_.load(std::memory_order_seq_cst) != view) {
                guard.Clear();
                RetireOldViews();
                continue;
            }

            uint32_t copied_length = 0;
            const int lookup_result = LookupValue(view, key, key_length, value,
                copied_length);
            const bool stable
                = header_->map_generation.load(std::memory_order_acquire)
                        == map_generation
                && (map_generation & 1U) == 0
                && header_->value_generation.load(std::memory_order_acquire)
                        == value_generation
                && current_view_.load(std::memory_order_seq_cst) == view;
            if (!stable) {
                value.data_len = 0;
                guard.Clear();
                RetireOldViews();
                continue;
            }
            if (lookup_result == MBError::SUCCESS)
                value.data_len = static_cast<int>(copied_length);
            return lookup_result;
        }
        value.data_len = 0;
        return MBError::TRY_AGAIN;
    }

    int Erase(const uint8_t* key, int key_length)
    {
        if (!writer_)
            return MBError::NOT_ALLOWED;
        if (key == nullptr || key_length <= 0
            || key_length > CONSTS::MAX_KEY_LENGHTH) {
            return MBError::INVALID_ARG;
        }

        const uint64_t generation
            = header_->value_generation.load(std::memory_order_acquire);
        ValueGenerationView* view = EnsureView(generation);
        if (view == nullptr)
            return MBError::READ_ERROR;
        const uint64_t hash = HashMapImpl::normalize_hash(
            HashMapImpl::fnv1a64(key, key_length));
        ProbeResult probe;
        int result = ProbeForWriter(view, key, key_length, hash, probe);
        if (result != MBError::SUCCESS)
            return result;
        if (!probe.found)
            return MBError::NOT_EXIST;
        if (!CanRetire(probe.old_size))
            return MBError::TRY_AGAIN;

        std::vector<BucketMove> moves;
        size_t final_hole = probe.index;
        result = BuildEraseShiftPlan(view, probe.index, moves, final_hole);
        if (result != MBError::SUCCESS)
            return result;

        uint64_t map_generation = 0;
        if (!moves.empty()) {
            map_generation
                = header_->map_generation.load(std::memory_order_relaxed);
            if ((map_generation & 1U) != 0)
                return MBError::TRY_AGAIN;
            if (map_generation
                > std::numeric_limits<uint64_t>::max() - 2) {
                return MBError::NO_RESOURCE;
            }
        }

        const uint64_t retire_epoch
            = header_->reader_epoch.load(std::memory_order_seq_cst);
        try {
            retired_.push_back({ generation, probe.old_offset, probe.old_size,
                retire_epoch });
        } catch (const std::bad_alloc&) {
            return MBError::NO_MEMORY;
        }

        if (!moves.empty()) {
            header_->map_generation.store(map_generation + 1,
                std::memory_order_release);
        }
        for (const BucketMove& move : moves) {
            Bucket(move.target)->entry.store(move.entry,
                std::memory_order_release);
        }
        Bucket(final_hole)->entry.store(kEmptyValueEntry,
            std::memory_order_release);
        retired_bytes_ += probe.old_size;
        header_->retired_value_bytes.store(retired_bytes_,
            std::memory_order_relaxed);
        header_->live_value_bytes.fetch_sub(probe.old_size,
            std::memory_order_relaxed);
        header_->used.fetch_sub(1, std::memory_order_relaxed);
        if (!moves.empty()) {
            header_->map_generation.store(map_generation + 2,
                std::memory_order_release);
        }
        MaybeReclaim();
        return MBError::SUCCESS;
    }

    void PrintStats(std::ostream& output) const
    {
        output << "HashMap value stats:\n"
               << "\tcapacity: " << header_->capacity << "\n"
               << "\tused: "
               << header_->used.load(std::memory_order_relaxed) << "\n"
               << "\ttombstones: "
               << header_->tombstones.load(std::memory_order_relaxed) << "\n"
               << "\tvalue generation: "
               << header_->value_generation.load(std::memory_order_relaxed)
               << "\n"
               << "\tlive value bytes: "
               << header_->live_value_bytes.load(std::memory_order_relaxed)
               << "\n"
               << "\tretired value bytes: "
               << header_->retired_value_bytes.load(std::memory_order_relaxed)
               << "\n";
    }

    void Flush()
    {
        map_.file_.Flush();
        if (writer_file_ != nullptr)
            writer_file_->Flush();
    }

    void ReleaseSlot(size_t slot_index, uint64_t owner_id)
    {
        if (header_ == nullptr || slot_index >= header_->reader_slot_count)
            return;
        HashMapImpl::ValueReaderSlot& slot = header_->reader_slots[slot_index];
        uint64_t expected = owner_id;
        if (!slot.owner_id.compare_exchange_strong(expected, kSlotClaiming,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return;
        }
        slot.active_epoch.store(0, std::memory_order_seq_cst);
        slot.active_value_generation.store(0, std::memory_order_relaxed);
        slot.process_id.store(0, std::memory_order_relaxed);
        slot.process_start_time.store(0, std::memory_order_relaxed);
        if (local_owners_ != nullptr)
            local_owners_[slot_index].store(0, std::memory_order_release);
        slot.owner_id.store(0, std::memory_order_release);
    }

private:
    class ActiveSlotGuard {
    public:
        explicit ActiveSlotGuard(HashMapImpl::ValueReaderSlot& slot)
            : slot_(slot)
            , active_(false)
        {
        }

        ~ActiveSlotGuard()
        {
            if (active_) {
                slot_.active_epoch.store(0, std::memory_order_seq_cst);
                slot_.active_value_generation.store(0,
                    std::memory_order_relaxed);
            }
        }

        void Publish(uint64_t generation, uint64_t epoch)
        {
            slot_.active_value_generation.store(generation,
                std::memory_order_release);
            slot_.active_epoch.store(epoch, std::memory_order_seq_cst);
            active_ = true;
        }

        void Clear()
        {
            if (!active_)
                return;
            slot_.active_epoch.store(0, std::memory_order_seq_cst);
            slot_.active_value_generation.store(0,
                std::memory_order_relaxed);
            active_ = false;
        }

    private:
        HashMapImpl::ValueReaderSlot& slot_;
        bool active_;
    };

    struct ProbeResult {
        bool found = false;
        bool reused_tombstone = false;
        size_t index = 0;
        size_t old_offset = 0;
        size_t old_size = 0;
    };

    struct RetiredRecord {
        uint64_t generation;
        size_t offset;
        size_t size;
        uint64_t retirement_epoch;
    };

    struct BucketMove {
        size_t target;
        uint64_t entry;
    };

    void AcquireWriterLock()
    {
        const std::string lock_path = map_.path_ + ".lock";
        lock_fd_ = open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC,
            kOwnerFileMode);
        if (lock_fd_ < 0)
            throw static_cast<int>(MBError::OPEN_FAILURE);
        if (fchmod(lock_fd_, kOwnerFileMode) != 0) {
            close(lock_fd_);
            lock_fd_ = -1;
            throw static_cast<int>(MBError::WRITE_ERROR);
        }
        if (flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
            const int error = errno;
            close(lock_fd_);
            lock_fd_ = -1;
            throw static_cast<int>(
                error == EWOULDBLOCK || error == EAGAIN
                    ? MBError::WRITER_EXIST
                    : MBError::MUTEX_ERROR);
        }
    }

    std::string ValuePath(uint64_t generation) const
    {
        return map_.path_ + "_values_g" + std::to_string(generation) + "_";
    }

    bool ValidInput(const uint8_t* key, int key_length, const uint8_t* value,
        int value_length) const
    {
        if (key == nullptr || value == nullptr || key_length <= 0
            || value_length <= 0 || key_length > CONSTS::MAX_KEY_LENGHTH
            || value_length > CONSTS::MAX_DATA_SIZE) {
            return false;
        }
        size_t total = 0;
        return RecordSize(static_cast<uint32_t>(key_length),
                   static_cast<uint32_t>(value_length), total)
            && total <= config_.value_block_size;
    }

    size_t MaximumOccupancy() const
    {
        const size_t quotient = header_->capacity / 100U;
        const size_t remainder = header_->capacity % 100U;
        return quotient * header_->max_load_percent
            + remainder * header_->max_load_percent / 100U;
    }

    HashMapImpl::ValueBucket* Bucket(size_t index) const
    {
        return reinterpret_cast<HashMapImpl::ValueBucket*>(
            map_.map_base_ + header_->buckets_off
            + index * sizeof(HashMapImpl::ValueBucket));
    }

    RecordMatch InspectRecord(ValueGenerationView* view, size_t offset,
        const uint8_t* key, int key_length, MBData* output,
        size_t& record_size, uint32_t& value_length)
    {
        record_size = 0;
        value_length = 0;
        if (offset == 0 || offset >= config_.value_memcap)
            return RecordMatch::ERROR;
        uint8_t* raw_header = view->Resolve(offset, sizeof(ValueRecordHeader));
        if (raw_header == nullptr)
            return RecordMatch::ERROR;

        ValueRecordHeader header {};
        std::memcpy(&header, raw_header, sizeof(header));
        if (header.flags != 0 || header.key_length == 0
            || header.key_length > CONSTS::MAX_KEY_LENGHTH
            || header.value_length == 0
            || header.value_length > static_cast<uint32_t>(
                   CONSTS::MAX_DATA_SIZE)
            || !RecordSize(header.key_length, header.value_length, record_size)
            || record_size > config_.value_memcap
            || offset > config_.value_memcap - record_size) {
            return RecordMatch::ERROR;
        }
        uint8_t* record = view->Resolve(offset, record_size);
        if (record == nullptr)
            return RecordMatch::ERROR;
        if (header.key_length != static_cast<uint32_t>(key_length)
            || std::memcmp(record + sizeof(header), key,
                   static_cast<size_t>(key_length))
                != 0) {
            return RecordMatch::DIFFERENT;
        }

        value_length = header.value_length;
        if (output != nullptr) {
            if (output->Resize(static_cast<int>(header.value_length))
                != MBError::SUCCESS) {
                return RecordMatch::NO_MEMORY;
            }
            const size_t value_offset
                = sizeof(header) + static_cast<size_t>(header.key_length);
            std::memcpy(output->buff, record + value_offset,
                header.value_length);
        }
        return RecordMatch::MATCH;
    }

    int RecordHash(ValueGenerationView* view, uint64_t entry, uint64_t& hash)
    {
        const size_t offset = ValueEntryOffset(entry);
        if (offset == 0 || offset >= config_.value_memcap)
            return MBError::READ_ERROR;
        uint8_t* raw_header = view->Resolve(offset, sizeof(ValueRecordHeader));
        if (raw_header == nullptr)
            return MBError::READ_ERROR;

        ValueRecordHeader header {};
        std::memcpy(&header, raw_header, sizeof(header));
        size_t record_size = 0;
        if (header.flags != 0 || header.key_length == 0
            || header.key_length > CONSTS::MAX_KEY_LENGHTH
            || header.value_length == 0
            || header.value_length > static_cast<uint32_t>(
                   CONSTS::MAX_DATA_SIZE)
            || !RecordSize(header.key_length, header.value_length, record_size)
            || record_size > config_.value_memcap
            || offset > config_.value_memcap - record_size) {
            return MBError::READ_ERROR;
        }
        uint8_t* record = view->Resolve(offset, record_size);
        if (record == nullptr)
            return MBError::READ_ERROR;
        hash = HashMapImpl::normalize_hash(HashMapImpl::fnv1a64(
            record + sizeof(header), static_cast<int>(header.key_length)));
        return ValueEntryHashMatches(entry, hash)
            ? MBError::SUCCESS
            : MBError::READ_ERROR;
    }

    int BuildEraseShiftPlan(ValueGenerationView* view, size_t erased_index,
        std::vector<BucketMove>& moves, size_t& final_hole)
    {
        size_t hole = erased_index;
        for (size_t scanned = 1; scanned < header_->capacity; ++scanned) {
            const size_t index = (erased_index + scanned) & header_->mask;
            const uint64_t entry
                = Bucket(index)->entry.load(std::memory_order_relaxed);
            if (entry == kEmptyValueEntry) {
                final_hole = hole;
                return MBError::SUCCESS;
            }
            // Writer initialization starts with no tombstones and this erase
            // path never creates one. Seeing one means the invariant was lost.
            if (entry == kTombstoneValueEntry)
                return MBError::READ_ERROR;

            uint64_t hash = 0;
            const int result = RecordHash(view, entry, hash);
            if (result != MBError::SUCCESS)
                return result;
            const size_t home = static_cast<size_t>(hash) & header_->mask;
            const size_t distance_to_hole = (hole - home) & header_->mask;
            const size_t distance_to_entry = (index - home) & header_->mask;
            if (distance_to_hole >= distance_to_entry)
                continue;
            try {
                moves.push_back({ hole, entry });
            } catch (const std::bad_alloc&) {
                return MBError::NO_MEMORY;
            }
            hole = index;
        }
        return MBError::READ_ERROR;
    }

    int ProbeForWriter(ValueGenerationView* view, const uint8_t* key,
        int key_length, uint64_t hash, ProbeResult& result)
    {
        const size_t start = static_cast<size_t>(hash) & header_->mask;
        size_t first_tombstone = header_->capacity;
        for (size_t probe = 0; probe < header_->capacity; ++probe) {
            const size_t index = (start + probe) & header_->mask;
            HashMapImpl::ValueBucket* bucket = Bucket(index);
            const uint64_t bucket_entry
                = bucket->entry.load(std::memory_order_relaxed);
            if (bucket_entry == kTombstoneValueEntry) {
                if (first_tombstone == header_->capacity)
                    first_tombstone = index;
                continue;
            }
            if (bucket_entry == kEmptyValueEntry) {
                result.index = first_tombstone != header_->capacity
                    ? first_tombstone
                    : index;
                result.reused_tombstone
                    = first_tombstone != header_->capacity;
                return MBError::SUCCESS;
            }
            if (!ValueEntryHashMatches(bucket_entry, hash))
                continue;

            const size_t offset = ValueEntryOffset(bucket_entry);
            size_t record_size = 0;
            uint32_t ignored_length = 0;
            const RecordMatch match = InspectRecord(view, offset, key,
                key_length, nullptr, record_size, ignored_length);
            if (match == RecordMatch::ERROR)
                return MBError::READ_ERROR;
            if (match == RecordMatch::MATCH) {
                result.found = true;
                result.index = index;
                result.old_offset = offset;
                result.old_size = record_size;
                return MBError::SUCCESS;
            }
        }
        if (first_tombstone != header_->capacity) {
            result.index = first_tombstone;
            result.reused_tombstone = true;
            return MBError::SUCCESS;
        }
        return MBError::NO_RESOURCE;
    }

    int LookupValue(ValueGenerationView* view, const uint8_t* key,
        int key_length, MBData& output, uint32_t& copied_length)
    {
        const uint64_t hash = HashMapImpl::normalize_hash(
            HashMapImpl::fnv1a64(key, key_length));
        const size_t start = static_cast<size_t>(hash) & header_->mask;
        for (size_t probe = 0; probe < header_->capacity; ++probe) {
            const size_t index = (start + probe) & header_->mask;
            if (probe >= 4 && probe + 2 < header_->capacity) {
                const size_t prefetch = (start + probe + 2) & header_->mask;
                __builtin_prefetch(Bucket(prefetch), 0, 1);
            }
            HashMapImpl::ValueBucket* bucket = Bucket(index);
            const uint64_t bucket_entry
                = bucket->entry.load(std::memory_order_acquire);
            if (bucket_entry == kEmptyValueEntry)
                return MBError::NOT_EXIST;
            if (bucket_entry == kTombstoneValueEntry
                || !ValueEntryHashMatches(bucket_entry, hash)) {
                continue;
            }

            const size_t offset = ValueEntryOffset(bucket_entry);
            size_t ignored_size = 0;
            uint32_t value_length = 0;
            const RecordMatch match = InspectRecord(view, offset, key,
                key_length, &output, ignored_size, value_length);
            if (match == RecordMatch::NO_MEMORY)
                return MBError::NO_MEMORY;
            if (match == RecordMatch::ERROR)
                return MBError::READ_ERROR;
            if (match == RecordMatch::MATCH) {
                copied_length = value_length;
                return MBError::SUCCESS;
            }
        }
        return MBError::NOT_EXIST;
    }

    void* AllocateRecord(const uint8_t* key, int key_length,
        const uint8_t* value, int value_length, size_t& offset,
        size_t& record_size)
    {
        if (!RecordSize(static_cast<uint32_t>(key_length),
                static_cast<uint32_t>(value_length), record_size)) {
            return nullptr;
        }
        void* allocation = writer_file_->Malloc(record_size, offset);
        if (allocation == nullptr) {
            header_->allocation_failures.fetch_add(1,
                std::memory_order_relaxed);
            return nullptr;
        }
        if (offset == 0 || offset >= config_.value_memcap
            || record_size > config_.value_memcap - offset
            || offset % config_.value_block_size
                    > config_.value_block_size - record_size) {
            writer_file_->Free(allocation);
            header_->allocation_failures.fetch_add(1,
                std::memory_order_relaxed);
            return nullptr;
        }

        ValueRecordHeader header {
            static_cast<uint32_t>(value_length),
            static_cast<uint16_t>(key_length), 0
        };
        uint8_t* destination = static_cast<uint8_t*>(allocation);
        std::memcpy(destination, &header, sizeof(header));
        std::memcpy(destination + sizeof(header), key,
            static_cast<size_t>(key_length));
        std::memcpy(destination + sizeof(header)
                + static_cast<size_t>(key_length),
            value, static_cast<size_t>(value_length));
        return allocation;
    }

    bool CanRetire(size_t size)
    {
        if (retired_bytes_ >= config_.reclaim_threshold_bytes
            || retired_bytes_ > config_.max_retired_bytes
            || size > config_.max_retired_bytes - retired_bytes_) {
            Reclaim();
        }
        return retired_bytes_ <= config_.max_retired_bytes
            && size <= config_.max_retired_bytes - retired_bytes_;
    }

    void MaybeReclaim()
    {
        if (retired_bytes_ >= config_.reclaim_threshold_bytes)
            Reclaim();
    }

    void Reclaim()
    {
        if (retired_.empty())
            return;
        CleanDeadSlots();
        uint64_t epoch = header_->reader_epoch.load(std::memory_order_seq_cst);
        if (epoch == std::numeric_limits<uint64_t>::max())
            return;
        const uint64_t advanced
            = header_->reader_epoch.fetch_add(1, std::memory_order_seq_cst) + 1;

        for (auto entry = retired_.begin(); entry != retired_.end();) {
            if (entry->retirement_epoch >= advanced
                || IsProtected(*entry)) {
                ++entry;
                continue;
            }
            try {
                writer_file_->Free(entry->offset);
            } catch (...) {
                ++entry;
                continue;
            }
            retired_bytes_ -= entry->size;
            entry = retired_.erase(entry);
        }
        header_->retired_value_bytes.store(retired_bytes_,
            std::memory_order_relaxed);
    }

    bool IsProtected(const RetiredRecord& record) const
    {
        for (uint32_t index = 0; index < header_->reader_slot_count; ++index) {
            const HashMapImpl::ValueReaderSlot& slot = header_->reader_slots[index];
            const uint64_t epoch
                = slot.active_epoch.load(std::memory_order_seq_cst);
            if (epoch == 0 || epoch > record.retirement_epoch)
                continue;
            if (slot.active_value_generation.load(std::memory_order_acquire)
                == record.generation) {
                return true;
            }
        }
        return false;
    }

    void CleanDeadSlots()
    {
        for (uint32_t index = 0; index < header_->reader_slot_count; ++index) {
            HashMapImpl::ValueReaderSlot& slot = header_->reader_slots[index];
            const uint64_t owner
                = slot.owner_id.load(std::memory_order_acquire);
            if (owner == 0 || owner == kSlotClaiming)
                continue;
            const uint64_t pid
                = slot.process_id.load(std::memory_order_acquire);
            const uint64_t start
                = slot.process_start_time.load(std::memory_order_acquire);
            const uint64_t epoch
                = slot.active_epoch.load(std::memory_order_seq_cst);
            const uint64_t generation = slot.active_value_generation.load(
                std::memory_order_acquire);
            if (ProcessInstanceIsAlive(pid, start))
                continue;

            uint64_t expected = owner;
            if (!slot.owner_id.compare_exchange_strong(expected, kSlotClaiming,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                continue;
            }
            if (slot.process_id.load(std::memory_order_acquire) != pid
                || slot.process_start_time.load(std::memory_order_acquire)
                    != start
                || slot.active_epoch.load(std::memory_order_seq_cst) != epoch
                || slot.active_value_generation.load(std::memory_order_acquire)
                    != generation) {
                slot.owner_id.store(owner, std::memory_order_release);
                continue;
            }
            slot.active_epoch.store(0, std::memory_order_seq_cst);
            slot.active_value_generation.store(0, std::memory_order_relaxed);
            slot.process_id.store(0, std::memory_order_relaxed);
            slot.process_start_time.store(0, std::memory_order_relaxed);
            slot.owner_id.store(0, std::memory_order_release);
        }
    }

    int GetThreadSlot(ClaimedSlot& output)
    {
        const int result = TryGetThreadSlot(output);
        if (result != MBError::TRY_AGAIN)
            return result;
        CleanDeadSlots();
        return TryGetThreadSlot(output);
    }

    int TryGetThreadSlot(ClaimedSlot& output)
    {
        ThreadSlotEntry* cached = g_thread_slots.Find(connection_id_);
        if (cached != nullptr && cached->slot_index < header_->reader_slot_count
            && header_->reader_slots[cached->slot_index].owner_id.load(
                   std::memory_order_acquire)
                == cached->owner_id) {
            output = { cached->slot_index, cached->owner_id };
            return MBError::SUCCESS;
        }

        uint64_t owner = Mix64(connection_id_
            ^ OwnerCounter().fetch_add(1, std::memory_order_relaxed)
            ^ static_cast<uint64_t>(
                std::hash<std::thread::id>()(std::this_thread::get_id())));
        if (owner == 0 || owner == kSlotClaiming)
            owner ^= 0xD6E8FEB86659FD93ULL;

        for (uint32_t index = 0; index < header_->reader_slot_count; ++index) {
            HashMapImpl::ValueReaderSlot& slot = header_->reader_slots[index];
            uint64_t expected = 0;
            if (!slot.owner_id.compare_exchange_strong(expected, kSlotClaiming,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                continue;
            }
            slot.process_id.store(process_id_, std::memory_order_relaxed);
            slot.process_start_time.store(process_start_time_,
                std::memory_order_relaxed);
            slot.active_value_generation.store(0,
                std::memory_order_relaxed);
            slot.active_epoch.store(0, std::memory_order_seq_cst);
            local_owners_[index].store(owner, std::memory_order_release);
            slot.owner_id.store(owner, std::memory_order_release);

            ThreadSlotEntry claimed {
                connection_id_, index, owner, connection_token_
            };
            try {
                if (cached != nullptr)
                    *cached = claimed;
                else
                    cached = g_thread_slots.Add(claimed);
            } catch (const std::bad_alloc&) {
                ReleaseSlot(index, owner);
                return MBError::NO_MEMORY;
            }
            output = { index, owner };
            return MBError::SUCCESS;
        }
        return MBError::TRY_AGAIN;
    }

    bool IsLocalGenerationActive(uint64_t generation) const
    {
        if (header_ == nullptr || local_owners_ == nullptr)
            return false;
        for (uint32_t index = 0; index < header_->reader_slot_count; ++index) {
            const uint64_t local_owner
                = local_owners_[index].load(std::memory_order_acquire);
            if (local_owner == 0)
                continue;
            const HashMapImpl::ValueReaderSlot& slot = header_->reader_slots[index];
            if (slot.owner_id.load(std::memory_order_acquire) != local_owner)
                continue;
            if (slot.active_epoch.load(std::memory_order_seq_cst) != 0
                && slot.active_value_generation.load(std::memory_order_acquire)
                    == generation) {
                return true;
            }
        }
        return false;
    }

    void RetireOldViews()
    {
        ValueGenerationView* current
            = current_view_.load(std::memory_order_seq_cst);
        for (ValueGenerationView* view
                 = views_head_.load(std::memory_order_acquire);
             view != nullptr; view = view->Next()) {
            if (view != current
                && !IsLocalGenerationActive(view->Generation())) {
                view->RetireMappings();
            }
        }
    }

    static void ReleaseSlotThunk(void* context, size_t slot_index,
        uint64_t owner_id)
    {
        static_cast<HashMapValueState*>(context)->ReleaseSlot(slot_index,
            owner_id);
    }

    void ClearOwnedSlots()
    {
        if (header_ == nullptr || local_owners_ == nullptr)
            return;
        for (uint32_t index = 0; index < header_->reader_slot_count; ++index) {
            const uint64_t owner
                = local_owners_[index].load(std::memory_order_acquire);
            if (owner != 0)
                ReleaseSlot(index, owner);
        }
    }

    HashMapImpl& map_;
    HashMapValueConfig config_;
    HashMapImpl::ValueHMHeader* header_;
    bool writer_;
    uint64_t process_id_;
    uint64_t process_start_time_;
    uint64_t connection_id_;
    std::shared_ptr<ConnectionToken> connection_token_;
    int lock_fd_;
    std::unique_ptr<std::atomic<uint64_t>[]> local_owners_;
    std::atomic<ValueGenerationView*> current_view_;
    std::atomic<ValueGenerationView*> views_head_;
    std::unique_ptr<RollableFile> writer_file_;
    uint64_t writer_generation_;
    std::deque<RetiredRecord> retired_;
    size_t retired_bytes_;
};

namespace {

void ValidateValueConfig(const HashMapValueConfig& config)
{
    size_t largest_record = 0;
    if (!RecordSize(CONSTS::MAX_KEY_LENGHTH, CONSTS::MAX_DATA_SIZE,
            largest_record)
        || config.value_block_size < kMinimumValueBlockSize
        || config.value_block_size < largest_record
        || config.value_block_size % kMinimumValueBlockSize != 0
        || config.value_memcap == 0
        || static_cast<uint64_t>(config.value_memcap) > kValueOffsetLimit
        || config.value_memcap % config.value_block_size != 0
        || config.value_memcap / config.value_block_size
            > std::numeric_limits<uint32_t>::max()
        || config.reader_slots == 0
        || config.reader_slots > kMaxValueReaderSlots
        || config.reclaim_threshold_bytes == 0
        || config.max_retired_bytes < config.reclaim_threshold_bytes
        || config.max_retired_bytes < largest_record
        || config.max_load_percent < 20 || config.max_load_percent > 90) {
        throw static_cast<int>(MBError::INVALID_SIZE);
    }
}

size_t ValueCapacity(size_t requested, size_t map_size, size_t header_size,
    size_t bucket_size)
{
    size_t capacity = 0;
    if (!NextPowerOfTwo(std::max(requested, kMinimumCapacity), capacity)
        || map_size < header_size
        || capacity > (map_size - header_size) / bucket_size) {
        throw static_cast<int>(MBError::INVALID_SIZE);
    }
    return capacity;
}

bool StatSizedFile(const std::string& path, size_t size, bool& exists)
{
    struct stat info {};
    if (stat(path.c_str(), &info) == 0) {
        exists = true;
        return info.st_size >= 0
            && static_cast<uint64_t>(info.st_size) == size;
    }
    exists = false;
    return errno == ENOENT;
}

} // namespace

HashMapImpl::HashMapImpl(const std::string& mbdir, size_t requested_capacity,
    int options, const HashMapValueConfig& config, size_t index_memcap_mb)
    : path_(mbdir + "_hashmap")
    , options_(options)
    , map_size_(checked_memcap_bytes(index_memcap_mb))
    , file_(path_, map_size_, map_size_, options, 1)
    , map_base_(nullptr)
    , hdr_(nullptr)
    , compact_(true)
    , storage_mode_(StorageMode::VALUE)
    , value_hdr_(nullptr)
    , value_state_(nullptr)
{
    ValidateValueConfig(config);
    if ((options & CONSTS::MEMORY_ONLY_MODE) != 0)
        throw static_cast<int>(MBError::NOT_ALLOWED);
    std::atomic<uint64_t> atomic64;
    std::atomic<size_t> atomic_size;
    if (!atomic64.is_lock_free() || !atomic_size.is_lock_free())
        throw static_cast<int>(MBError::NOT_ALLOWED);

    const bool writer = (options & CONSTS::ACCESS_MODE_WRITER) != 0;
    value_state_ = std::make_shared<HashMapValueState>(*this, config, writer);

    const std::string index_path = path_ + "0";
    bool exists = false;
    if (!StatSizedFile(index_path, map_size_, exists))
        throw static_cast<int>(exists ? MBError::INVALID_SIZE
                                      : MBError::OPEN_FAILURE);
    bool created = false;
    if (!exists) {
        if (!writer)
            throw static_cast<int>(MBError::NO_DB);
        int create_result = CreateExclusiveSizedFile(index_path, map_size_);
        if (create_result != MBError::SUCCESS)
            throw static_cast<int>(create_result);
        created = true;
    }

    uint8_t* base = nullptr;
    if (created) {
        size_t offset = 0;
        int result = file_.Reserve(offset,
            static_cast<int>(sizeof(ValueHMHeader)), base, true);
        if (result != MBError::SUCCESS || base == nullptr)
            throw static_cast<int>(result);
    } else {
        base = file_.GetShmPtr(0, static_cast<int>(sizeof(ValueHMHeader)));
        if (base == nullptr)
            throw static_cast<int>(MBError::MMAP_FAILED);
    }
    map_base_ = base;
    value_hdr_ = reinterpret_cast<ValueHMHeader*>(map_base_);
    const size_t capacity = ValueCapacity(requested_capacity, map_size_,
        sizeof(ValueHMHeader), sizeof(ValueBucket));

    const uint64_t control = created
        ? 0
        : value_hdr_->control.load(std::memory_order_acquire);
    const bool initialize = control == 0;
    if (initialize) {
        if (!writer)
            throw static_cast<int>(MBError::NOT_INITIALIZED);
        new (&value_hdr_->control) std::atomic<uint64_t>(0);
        value_hdr_->capacity = capacity;
        value_hdr_->mask = capacity - 1;
        value_hdr_->buckets_off = sizeof(ValueHMHeader);
        value_hdr_->value_block_size = config.value_block_size;
        value_hdr_->value_memcap = config.value_memcap;
        value_hdr_->max_value_blocks = static_cast<uint32_t>(
            config.value_memcap / config.value_block_size);
        value_hdr_->reader_slot_count = config.reader_slots;
        value_hdr_->hash_algorithm = kHashAlgorithm;
        value_hdr_->max_load_percent = config.max_load_percent;
        value_hdr_->max_key_length = CONSTS::MAX_KEY_LENGHTH;
        value_hdr_->max_value_length = CONSTS::MAX_DATA_SIZE;
        new (&value_hdr_->map_generation) std::atomic<uint64_t>(1);
        new (&value_hdr_->value_generation) std::atomic<uint64_t>(0);
        new (&value_hdr_->reader_epoch)
            std::atomic<uint64_t>(kFirstReaderEpoch);
        new (&value_hdr_->used) std::atomic<uint64_t>(0);
        new (&value_hdr_->tombstones) std::atomic<uint64_t>(0);
        new (&value_hdr_->live_value_bytes) std::atomic<uint64_t>(0);
        new (&value_hdr_->retired_value_bytes) std::atomic<uint64_t>(0);
        new (&value_hdr_->allocation_failures) std::atomic<uint64_t>(0);
        for (uint32_t index = 0; index < MAX_VALUE_READER_SLOTS; ++index) {
            ValueReaderSlot& slot = value_hdr_->reader_slots[index];
            new (&slot.owner_id) std::atomic<uint64_t>(0);
            new (&slot.process_id) std::atomic<uint64_t>(0);
            new (&slot.process_start_time) std::atomic<uint64_t>(0);
            new (&slot.active_epoch) std::atomic<uint64_t>(0);
            new (&slot.active_value_generation) std::atomic<uint64_t>(0);
        }
        for (size_t index = 0; index < capacity; ++index) {
            ValueBucket* bucket = reinterpret_cast<ValueBucket*>(
                map_base_ + value_hdr_->buckets_off
                + index * sizeof(ValueBucket));
            new (&bucket->entry) std::atomic<uint64_t>(kEmptyValueEntry);
        }
    } else {
        if (control != kValueControl)
            throw static_cast<int>(MBError::VERSION_MISMATCH);
        if (value_hdr_->capacity != capacity
            || value_hdr_->mask != capacity - 1
            || value_hdr_->buckets_off != sizeof(ValueHMHeader)
            || value_hdr_->value_block_size != config.value_block_size
            || value_hdr_->value_memcap != config.value_memcap
            || value_hdr_->max_value_blocks
                != config.value_memcap / config.value_block_size
            || value_hdr_->reader_slot_count != config.reader_slots
            || value_hdr_->hash_algorithm != kHashAlgorithm
            || value_hdr_->max_load_percent != config.max_load_percent
            || value_hdr_->max_key_length
                != static_cast<uint32_t>(CONSTS::MAX_KEY_LENGHTH)
            || value_hdr_->max_value_length
                != static_cast<uint32_t>(CONSTS::MAX_DATA_SIZE)) {
            throw static_cast<int>(MBError::INVALID_SIZE);
        }
    }

    value_state_->Attach(value_hdr_);
    if (writer) {
        const uint64_t stored_generation = initialize
            ? 0
            : value_hdr_->value_generation.load(std::memory_order_acquire);
        const uint64_t new_generation
            = value_state_->PrepareWriterGeneration(stored_generation);

        uint64_t generation
            = value_hdr_->map_generation.load(std::memory_order_relaxed);
        if (generation >= std::numeric_limits<uint64_t>::max() - 1)
            throw static_cast<int>(MBError::NO_RESOURCE);
        const uint64_t resetting
            = (generation & 1U) != 0 ? generation : generation + 1;
        value_hdr_->map_generation.store(resetting,
            std::memory_order_release);
        for (size_t index = 0; index < capacity; ++index) {
            ValueBucket* bucket = reinterpret_cast<ValueBucket*>(
                map_base_ + value_hdr_->buckets_off
                + index * sizeof(ValueBucket));
            bucket->entry.store(kEmptyValueEntry, std::memory_order_release);
        }
        value_hdr_->used.store(0, std::memory_order_relaxed);
        value_hdr_->tombstones.store(0, std::memory_order_relaxed);
        value_hdr_->live_value_bytes.store(0, std::memory_order_relaxed);
        value_hdr_->retired_value_bytes.store(0,
            std::memory_order_relaxed);
        value_hdr_->value_generation.store(new_generation,
            std::memory_order_release);
        value_state_->InstallView(new_generation);
        value_hdr_->map_generation.store(resetting + 1,
            std::memory_order_release);
        if (initialize)
            value_hdr_->control.store(kValueControl, std::memory_order_release);
        value_state_->CleanupGenerationFiles(new_generation);
    } else {
        const uint64_t generation
            = value_hdr_->value_generation.load(std::memory_order_acquire);
        if (generation != 0)
            value_state_->InstallView(generation);
    }
}

int HashMapImpl::PutValue(const uint8_t* key, int key_length,
    const uint8_t* value, int value_length, bool overwrite)
{
    if (storage_mode_ != StorageMode::VALUE || value_state_ == nullptr)
        return MBError::NOT_ALLOWED;
    return value_state_->Put(key, key_length, value, value_length, overwrite);
}

int HashMapImpl::GetValue(const uint8_t* key, int key_length, MBData& value) const
{
    value.data_len = 0;
    if (storage_mode_ != StorageMode::VALUE || value_state_ == nullptr)
        return MBError::NOT_ALLOWED;
    return value_state_->Get(key, key_length, value);
}

int HashMapImpl::erase_value(const uint8_t* key, int length)
{
    return value_state_ == nullptr
        ? MBError::NOT_ALLOWED
        : value_state_->Erase(key, length);
}

void HashMapImpl::print_value_stats(std::ostream& output) const
{
    if (value_state_ != nullptr)
        value_state_->PrintStats(output);
}

void HashMapImpl::flush_value() const
{
    if (value_state_ != nullptr)
        value_state_->Flush();
}

} // namespace mabain
