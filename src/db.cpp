/*
 * Copyright (C) 2025 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @author Changxue Deng <chadeng@cisco.com>

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/file.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdlib>
#include <errno.h>

#include "async_writer.h"
#include "db.h"
#include "detail/search_engine.h"
#include "dict.h"
#include "drm_base.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "logger.h"
#include "mb_backup.h"
#include "mb_lsq.h"
#include "mb_rc.h"
#include "resource_pool.h"
#include "util/shm_mutex.h"
#include "util/utils.h"
#include "version.h"

namespace mabain {

namespace {

const uint64_t kRebuildBarrierGuardToken = static_cast<uint64_t>(-1);

int OpenRebuildGuardFd(const std::string& mb_dir, int options)
{
    const int flags = (options & CONSTS::ACCESS_MODE_WRITER) ? O_RDWR : O_RDONLY;
    return open((mb_dir + "_mabain_h").c_str(), flags);
}

int WaitForRebuildBarrierLock(int fd, int op)
{
    if (fd < 0)
        return MBError::NOT_INITIALIZED;
    while (flock(fd, op) != 0) {
        if (errno == EINTR)
            continue;
        return MBError::MUTEX_ERROR;
    }
    return MBError::SUCCESS;
}

void UnlockRebuildBarrier(int fd)
{
    if (fd < 0)
        return;
    while (flock(fd, LOCK_UN) != 0) {
        if (errno != EINTR)
            break;
    }
}

#ifdef __linux__
bool ReadProcStartTimeFromStatPath(const std::string& stat_path, uint64_t& start_time)
{
    std::ifstream stat_file(stat_path);
    if (!stat_file.is_open())
        return false;

    std::string stat_line;
    std::getline(stat_file, stat_line);
    if (stat_line.empty())
        return false;

    const size_t rparen = stat_line.rfind(')');
    if (rparen == std::string::npos || (rparen + 2) >= stat_line.size())
        return false;

    std::istringstream fields(stat_line.substr(rparen + 2));
    std::string token;
    for (int i = 0; i < 20; i++) {
        if (!(fields >> token))
            return false;
    }

    char* end = NULL;
    const unsigned long long value = std::strtoull(token.c_str(), &end, 10);
    if (end == token.c_str() || *end != '\0')
        return false;

    start_time = static_cast<uint64_t>(value);
    return true;
}

uint64_t ReadSelfProcStartTime()
{
    uint64_t start_time = 0;
    return ReadProcStartTimeFromStatPath("/proc/self/stat", start_time) ? start_time : 0;
}
#else
uint64_t ReadSelfProcStartTime()
{
    return 0;
}
#endif

void MaybeHoldRebuildGuardForExperiment()
{
    static const int hold_us = []() {
        const char* env = getenv("MABAIN_REBUILD_GUARD_HOLD_US");
        return env == NULL ? 0 : atoi(env);
    }();
    if (hold_us > 0)
        usleep(static_cast<useconds_t>(hold_us));
}

} // namespace

// Current mabain version 1.7.0
uint16_t version[4] = { 1, 7, 0, 0 };

DB::~DB()
{
    if (status != MBError::DB_CLOSED)
        Close();
}

int DB::Close()
{
    int rval = MBError::SUCCESS;

    if ((options & CONSTS::ACCESS_MODE_WRITER) && async_writer != NULL) {
        rval = async_writer->StopAsyncThread();
        if (rval != MBError::SUCCESS) {
            Logger::Log(LOG_LEVEL_WARN, "failed to stop async writer thread: %s",
                MBError::get_error_str(rval));
        }

        delete async_writer;
        async_writer = NULL;
    }

    if (dict != NULL) {
        UpdateNumHandlers(options, -1);

        dict->Destroy();
        delete dict;
        dict = NULL;
    } else {
        rval = status;
    }

    if (rebuild_guard_fd >= 0) {
        close(rebuild_guard_fd);
        rebuild_guard_fd = -1;
    }

    status = MBError::DB_CLOSED;
    if (options & CONSTS::ACCESS_MODE_WRITER) {
        release_file_lock(writer_lock_fd);
        std::string lock_file = mb_dir + "_lock";
        ResourcePool::getInstance().RemoveResourceByPath(lock_file);
    }
    Logger::Log(LOG_LEVEL_DEBUG, "connector %u disconnected from DB", identifier);
    return rval;
}

int DB::UpdateNumHandlers(int mode, int delta)
{
    int rval = MBError::SUCCESS;

    if (mode & CONSTS::ACCESS_MODE_WRITER)
        rval = dict->UpdateNumWriter(delta);
    else
        dict->UpdateNumReader(delta);

    return rval;
}

uint64_t DB::BeginReaderEpochGuard() const
{
    if (dict == NULL)
        return 0;

    IndexHeader* header = dict->GetHeaderPtr();
    if (header == NULL
        || !header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER))
        return 0;

    const uint32_t slot_connect_id = identifier != 0 ? identifier : static_cast<uint32_t>(getpid());
    for (uint32_t i = 0; i < header->reader_epoch_slot_count; i++) {
        ReaderEpochSlot& slot = header->reader_epoch_slot[i];
        uint32_t expected = 0;
        if (!slot.connect_id.compare_exchange_strong(
                expected, slot_connect_id, MEMORY_ORDER_WRITER, MEMORY_ORDER_READER))
            continue;

        slot.pid.store(static_cast<uint32_t>(getpid()), MEMORY_ORDER_WRITER);
        slot.proc_start_time.store(process_start_time, MEMORY_ORDER_WRITER);
        for (;;) {
            uint64_t epoch = header->reader_epoch.load(MEMORY_ORDER_READER);
            slot.epoch.store(epoch, MEMORY_ORDER_WRITER);
            reader_guard_fast_slot_count++;
            if (epoch == header->reader_epoch.load(MEMORY_ORDER_READER)) {
                MaybeHoldRebuildGuardForExperiment();
                return i + 1;
            }
        }
    }

    if (AcquireRebuildBarrierShared() != MBError::SUCCESS) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to acquire rebuild reader barrier fallback");
        return 0;
    }
    header = dict->GetHeaderPtr();
    if (header == NULL
        || !header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER)) {
        ReleaseRebuildBarrierShared();
        return 0;
    }
    reader_guard_barrier_fallback_count++;
    MaybeHoldRebuildGuardForExperiment();
    return kRebuildBarrierGuardToken;
}

void DB::EndReaderEpochGuard(uint64_t epoch) const
{
    if (epoch == 0)
        return;

    if (epoch == kRebuildBarrierGuardToken) {
        ReleaseRebuildBarrierShared();
        return;
    }

    if (dict == NULL)
        return;

    IndexHeader* header = dict->GetHeaderPtr();
    if (header == NULL)
        return;

    const uint64_t slot_index = epoch - 1;
    if (slot_index < header->reader_epoch_slot_count)
        header->reader_epoch_slot[slot_index].Clear();
}

int DB::EnsureRebuildGuardFd() const
{
    if (rebuild_guard_fd >= 0)
        return MBError::SUCCESS;
    if (options & CONSTS::MEMORY_ONLY_MODE)
        return MBError::NOT_INITIALIZED;

    rebuild_guard_fd = OpenRebuildGuardFd(mb_dir, options);
    if (rebuild_guard_fd < 0)
        return MBError::OPEN_FAILURE;
    return MBError::SUCCESS;
}

int DB::AcquireRebuildBarrierShared() const
{
    int rval = EnsureRebuildGuardFd();
    if (rval != MBError::SUCCESS)
        return rval;
    return WaitForRebuildBarrierLock(rebuild_guard_fd, LOCK_SH);
}

void DB::ReleaseRebuildBarrierShared() const
{
    UnlockRebuildBarrier(rebuild_guard_fd);
}

int DB::AcquireRebuildBarrierExclusive() const
{
    int rval = EnsureRebuildGuardFd();
    if (rval != MBError::SUCCESS)
        return rval;
    return WaitForRebuildBarrierLock(rebuild_guard_fd, LOCK_EX);
}

uint64_t DB::GetReaderGuardFastSlotCount() const
{
    return reader_guard_fast_slot_count;
}

uint64_t DB::GetReaderGuardBarrierFallbackCount() const
{
    return reader_guard_barrier_fallback_count;
}

void DB::ReleaseRebuildBarrierExclusive() const
{
    UnlockRebuildBarrier(rebuild_guard_fd);
}

// Constructor for initializing DB handle
DB::DB(const char* db_path,
    int db_options,
    size_t memcap_index,
    size_t memcap_data,
    uint32_t id,
    uint32_t queue_size)
    : status(MBError::NOT_INITIALIZED)
    , rebuild_guard_fd(-1)
    , process_start_time(0)
    , reader_guard_fast_slot_count(0)
    , reader_guard_barrier_fallback_count(0)
    , writer_lock_fd(-1)
{
    MBConfig config;
    memset(&config, 0, sizeof(config));
    config.mbdir = db_path;
    config.options = db_options;
    config.memcap_index = memcap_index;
    config.memcap_data = memcap_data;
    config.connect_id = id;
    config.queue_size = queue_size;

    InitDB(config);
}

DB::DB(MBConfig& config)
    : status(MBError::NOT_INITIALIZED)
    , rebuild_guard_fd(-1)
    , process_start_time(0)
    , reader_guard_fast_slot_count(0)
    , reader_guard_barrier_fallback_count(0)
    , writer_lock_fd(-1)
{
    InitDB(config);
}

int DB::ValidateConfig(MBConfig& config)
{
    if (config.mbdir == NULL)
        return MBError::INVALID_ARG;

    if (config.memcap_index == 0)
        config.memcap_index = 2 * config.block_size_index;
    if (config.memcap_data == 0)
        config.memcap_data = 2 * config.block_size_data;

    if (config.options & CONSTS::ACCESS_MODE_WRITER) {
        if (config.block_size_index == 0)
            config.block_size_index = INDEX_BLOCK_SIZE_DEFAULT;
        if (config.block_size_data == 0)
            config.block_size_data = DATA_BLOCK_SIZE_DEFAULT;
        if (config.num_entry_per_bucket <= 0)
            config.num_entry_per_bucket = 500;
        if (config.num_entry_per_bucket < 8) {
            std::cerr << "count in eviction bucket must be greater than 7\n";
            return MBError::INVALID_ARG;
        }

        if (config.options & CONSTS::OPTION_JEMALLOC) {
            if (config.memcap_index != config.block_size_index * config.max_num_index_block
                || config.memcap_data != config.block_size_data * config.max_num_data_block) {
                std::cout << "memcap must be equal to block size when using jemalloc\n";
                return MBError::INVALID_ARG;
            }
        }
    }
    if (config.options & CONSTS::USE_SLIDING_WINDOW) {
        std::cout << "sliding window option is deprecated\n";
        config.options &= ~CONSTS::USE_SLIDING_WINDOW;
    }

    if (config.block_size_index != 0 && (config.block_size_index % BLOCK_SIZE_ALIGN != 0)) {
        std::cerr << "block size must be multiple of " << BLOCK_SIZE_ALIGN << "\n";
        return MBError::INVALID_ARG;
    }
    if (config.block_size_data != 0 && (config.block_size_data % BLOCK_SIZE_ALIGN != 0)) {
        std::cerr << "block size must be multiple of " << BLOCK_SIZE_ALIGN << "\n";
        return MBError::INVALID_ARG;
    }

    if (config.max_num_index_block == 0)
        config.max_num_index_block = 1024;
    if (config.max_num_data_block == 0)
        config.max_num_data_block = 1024;
    if (config.queue_size > MB_MAX_NUM_SHM_QUEUE_NODE)
        std::cerr << "async queue size exceeds maximum\n";
    if (config.queue_size == 0 || config.queue_size > MB_MAX_NUM_SHM_QUEUE_NODE)
        config.queue_size = MB_MAX_NUM_SHM_QUEUE_NODE;
#ifdef __APPLE__
    if (config.queue_dir == nullptr)
        config.queue_dir = config.mbdir;
#endif

    return MBError::SUCCESS;
}

void DB::PreCheckDB(const MBConfig& config, bool& init_header, bool& update_header)
{
    if (config.options & CONSTS::ACCESS_MODE_WRITER) {
        std::string lock_file = mb_dir + "_lock";
        // internal check first
        int ret = ResourcePool::getInstance().AddResourceByPath(lock_file, NULL);
        if (ret == MBError::SUCCESS) {
            if (!(config.options & CONSTS::MEMORY_ONLY_MODE)) {
                // process check by file lock
                writer_lock_fd = acquire_file_lock_wait_n(lock_file, 1);
                if (writer_lock_fd < 0)
                    status = MBError::WRITER_EXIST;
            }
        } else {
            status = MBError::WRITER_EXIST;
        }
        if (status == MBError::WRITER_EXIST) {
            Logger::Log(LOG_LEVEL_ERROR, "failed to initialize db: %s",
                MBError::get_error_str(status));
            return;
        }
    }

    if (config.options & CONSTS::MEMORY_ONLY_MODE) {
        if (config.options & CONSTS::ACCESS_MODE_WRITER) {
            init_header = true;
        } else {
            init_header = false;
            if (!ResourcePool::getInstance().CheckExistence(mb_dir + "_mabain_h"))
                status = MBError::NO_DB;
        }
    } else {
        // Check if the DB directory exist with proper permission
        if (access(mb_dir.c_str(), F_OK)) {
            std::cerr << "database directory check for " + mb_dir + " failed errno: " + std::to_string(errno) << std::endl;
            status = MBError::NO_DB;
            return;
        }
        Logger::Log(LOG_LEVEL_DEBUG, "connector %u DB options: %d",
            config.connect_id, config.options);
        // Check if DB exist. This can be done by check existence of the first index file.
        // If this is the first time the DB is opened and it is in writer mode, then we
        // need to update the header for the first time. If only reader access mode is
        // required and the file does not exist, we should bail here and the DB open will
        // not be successful.
        std::string header_file = mb_dir + "_mabain_h";
        if (access(header_file.c_str(), R_OK)) {
            if (config.options & CONSTS::ACCESS_MODE_WRITER)
                init_header = true;
            else
                status = MBError::NO_DB;
        }
    }

    // Check Header version
    if (!init_header && !(config.options & CONSTS::MEMORY_ONLY_MODE)) {
        try {
            DRMBase::ValidateHeaderFile(mb_dir + "_mabain_h", config.options,
                config.queue_size, update_header);
        } catch (int error) {
            status = error;
            return;
        }
    }
}

void DB::PostDBUpdate(const MBConfig& config, bool init_header, bool update_header)
{
    if ((config.options & CONSTS::ACCESS_MODE_WRITER) && (init_header || update_header)) {
        if (init_header) {
            Logger::Log(LOG_LEVEL_DEBUG, "opened a new db %s", mb_dir.c_str());
        } else {
            Logger::Log(LOG_LEVEL_INFO, "converted %s to version %d.%d.%d", mb_dir.c_str(),
                version[0], version[1], version[2]);
        }
        IndexHeader* header = dict->GetHeaderPtr();
        if (header != NULL) {
            header->async_queue_size = config.queue_size;
            header->ResetReaderEpochState();
            header->jemalloc_index_free_start = 0;
            header->jemalloc_data_free_start = 0;
        }
        dict->Init(identifier);
    }

    if (dict->Status() != MBError::SUCCESS) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to iniitialize dict: %s ",
            MBError::get_error_str(dict->Status()));
        status = dict->Status();
        return;
    }

    process_start_time = ReadSelfProcStartTime();

    if (config.options & CONSTS::ACCESS_MODE_WRITER) {
        int rval = PrepareStartupRebuild(config, init_header);
        if (rval != MBError::SUCCESS) {
            status = rval;
            return;
        }
    }

    lock.Init(dict->GetShmLockPtr());
    UpdateNumHandlers(config.options, 1);

    if (config.options & CONSTS::ACCESS_MODE_WRITER) {
        if (config.options & CONSTS::ASYNC_WRITER_MODE)
            async_writer = AsyncWriter::CreateInstance(this);
    }

    if (!(init_header || update_header)) {
        IndexHeader* header = dict->GetHeaderPtr();
        if (header != NULL && header->async_queue_size != (int)config.queue_size) {
            Logger::Log(LOG_LEVEL_ERROR, "async queue size not matching with header: %d %d",
                header->async_queue_size, (int)config.queue_size);
            status = MBError::INVALID_SIZE;
            return;
        }
    }

    Logger::Log(LOG_LEVEL_DEBUG, "connector %u successfully opened DB %s for %s",
        identifier, mb_dir.c_str(), (config.options & CONSTS::ACCESS_MODE_WRITER) ? "writing" : "reading");
    status = MBError::SUCCESS;

    if (config.options & CONSTS::ACCESS_MODE_WRITER) {
        if (config.options & CONSTS::OPTION_JEMALLOC) {
            if (!init_header) {
                if (config.jemalloc_keep_db) {
                    Logger::Log(LOG_LEVEL_DEBUG,
                        "jemalloc mode: preserving existing db for startup rebuild");
                    int rval = RunStartupRebuild();
                    if (rval != MBError::SUCCESS) {
                        Logger::Log(LOG_LEVEL_ERROR, "startup rebuild failed: %s",
                            MBError::get_error_str(rval));
                        status = rval;
                        return;
                    }
                } else {
                    // Default behavior: reset db in jemalloc mode if header already exists
                    Logger::Log(LOG_LEVEL_DEBUG, "reset db in jemalloc mode");
                    int rval = dict->RemoveAll();
                    if (rval != MBError::SUCCESS) {
                        Logger::Log(LOG_LEVEL_ERROR, "failed to reset db: %s", MBError::get_error_str(rval));
                        status = rval;
                    }
                }
            }
        } else {
            if (!(config.options & CONSTS::ASYNC_WRITER_MODE)) {
                // Run rc exception recovery
                ResourceCollection rc(*this);
                int rval = rc.ExceptionRecovery();
                if (rval == MBError::SUCCESS) {
                    IndexHeader* header = dict->GetHeaderPtr();
                    header->excep_lf_offset = 0;
                    header->excep_offset = 0;
                    Logger::Log(LOG_LEVEL_DEBUG, "rc exception recovery successful");
                } else {
                    Logger::Log(LOG_LEVEL_WARN, "rc exception recovery failed: %s", MBError::get_error_str(rval));
                }
            }
        }
    }
}

bool DB::StartupRebuildRequested(const MBConfig& config, bool init_header) const
{
    return (config.options & CONSTS::ACCESS_MODE_WRITER)
        && (config.options & CONSTS::OPTION_JEMALLOC)
        && !init_header
        && config.jemalloc_keep_db;
}

int DB::PrepareStartupRebuild(const MBConfig& config, bool init_header)
{
    if (!StartupRebuildRequested(config, init_header))
        return MBError::SUCCESS;

    if (config.options & CONSTS::ASYNC_WRITER_MODE) {
        Logger::Log(LOG_LEVEL_ERROR,
            "jemalloc startup rebuild does not support async writer mode");
        return MBError::NOT_ALLOWED;
    }

    IndexHeader* header = dict->GetHeaderPtr();
    if (header == NULL)
        return MBError::NOT_INITIALIZED;

    if (!header->RebuildInProgress()) {
        header->ResetRebuildMetadata(REBUILD_STATE_PREP);
        Logger::Log(LOG_LEVEL_INFO, "jemalloc startup rebuild entering PREP for %s",
            mb_dir.c_str());
    } else {
        Logger::Log(LOG_LEVEL_INFO, "jemalloc startup rebuild resuming state %u for %s",
            header->rebuild_state, mb_dir.c_str());
    }

    return MBError::SUCCESS;
}

bool DB::StartupRebuildMetadataReady() const
{
    IndexHeader* header = dict == NULL ? NULL : dict->GetHeaderPtr();
    if (header == NULL)
        return false;
    if (header->rebuild_state == REBUILD_STATE_PREP)
        return false;
    if (header->rebuild_state != REBUILD_STATE_COPY
        && header->rebuild_state != REBUILD_STATE_CUTOVER)
        return false;
    if (header->rebuild_index_alloc_end == 0 || header->rebuild_data_alloc_end == 0
        || header->rebuild_index_source_end == 0 || header->rebuild_data_source_end == 0)
        return false;
    if (header->rebuild_index_source_end < header->rebuild_index_alloc_end
        || header->rebuild_data_source_end < header->rebuild_data_alloc_end)
        return false;
    return true;
}

bool DB::StartupRebuildComplete() const
{
    IndexHeader* header = dict == NULL ? NULL : dict->GetHeaderPtr();
    if (header == NULL || !StartupRebuildMetadataReady())
        return false;

    return header->rebuild_index_block_cursor >= header->rebuild_index_source_end
        && header->rebuild_data_block_cursor >= header->rebuild_data_source_end
        && header->reusable_index_block_count == 0
        && header->reusable_data_block_count == 0;
}

int DB::RunStartupRebuild()
{
    try {
        if (!(options & CONSTS::ACCESS_MODE_WRITER)
            || !(options & CONSTS::OPTION_JEMALLOC))
            return MBError::NOT_ALLOWED;
        if (dict == NULL)
            return MBError::NOT_INITIALIZED;
        IndexHeader* header = dict->GetHeaderPtr();
        if (header == NULL)
            return MBError::NOT_INITIALIZED;
        if (!header->RebuildInProgress())
            return MBError::SUCCESS;

        if (header->excep_updating_status != EXCEP_STATUS_NONE) {
            int rval = dict->ExceptionRecovery();
            if (rval != MBError::SUCCESS)
                return rval;
            header->excep_lf_offset = 0;
            header->excep_offset = 0;
        }

        ResourceCollection rc(*this);
        if (header->rebuild_state == REBUILD_STATE_PREP) {
            int rval = rc.StartupShrink();
            if (rval != MBError::SUCCESS)
                return rval;
        } else if (!StartupRebuildMetadataReady()) {
            Logger::Log(LOG_LEVEL_ERROR,
                "startup rebuild metadata incomplete for resumed state %u", header->rebuild_state);
            return MBError::INVALID_SIZE;
        }

        uint32_t stalled_retry_count = 0;
        const uint32_t startup_rebuild_stall_retry_limit = 60000;
        while (!StartupRebuildComplete()) {
            const size_t index_cursor = header->rebuild_index_block_cursor;
            const size_t data_cursor = header->rebuild_data_block_cursor;
            const uint32_t index_reusable = header->reusable_index_block_count;
            const uint32_t data_reusable = header->reusable_data_block_count;

            int rval = rc.StartupEvacuate();
            if (rval != MBError::SUCCESS)
                return rval;
            if (!StartupRebuildComplete()
                && index_cursor == header->rebuild_index_block_cursor
                && data_cursor == header->rebuild_data_block_cursor
                && index_reusable == header->reusable_index_block_count
                && data_reusable == header->reusable_data_block_count) {
                if (++stalled_retry_count > startup_rebuild_stall_retry_limit) {
                    Logger::Log(LOG_LEVEL_ERROR,
                        "startup rebuild timed out waiting for progress index_cursor=%llu/%llu data_cursor=%llu/%llu reusable=%u/%u",
                        static_cast<unsigned long long>(header->rebuild_index_block_cursor),
                        static_cast<unsigned long long>(header->rebuild_index_source_end),
                        static_cast<unsigned long long>(header->rebuild_data_block_cursor),
                        static_cast<unsigned long long>(header->rebuild_data_source_end),
                        header->reusable_index_block_count,
                        header->reusable_data_block_count);
                    return MBError::TIMEOUT;
                }
                usleep(1000);
            } else {
                stalled_retry_count = 0;
            }
        }

        header->pending_index_buff_size = 0;
        header->pending_data_buff_size = 0;
        header->reader_epoch_tracking_active.store(0, MEMORY_ORDER_WRITER);
        header->jemalloc_index_free_start = header->rebuild_index_alloc_end;
        header->jemalloc_data_free_start = header->rebuild_data_alloc_end;
        header->ClearRebuildMetadata();
        Logger::Log(LOG_LEVEL_INFO, "jemalloc startup rebuild completed for %s", mb_dir.c_str());
        return MBError::SUCCESS;
    } catch (int err) {
        Logger::Log(LOG_LEVEL_ERROR, "startup rebuild failed with exception: %s",
            MBError::get_error_str(err));
        return err;
    } catch (...) {
        Logger::Log(LOG_LEVEL_ERROR, "startup rebuild failed with unknown exception");
        return MBError::UNKNOWN_ERROR;
    }
}

void DB::ReInit(MBConfig& config)
{
    std::cout << "failed to open db with error: " << MBError::get_error_str(status) << "\n";
    std::cout << "erase corrupted DB and retry\n";
    Close();
    std::string db_dir = std::string(config.mbdir);
    remove_db_files(db_dir);
    status = MBError::NOT_INITIALIZED;
    InitDBEx(config);
}

void DB::InitDB(MBConfig& config)
{
    if (config.mbdir == nullptr)
        return;
    std::string db_dir = std::string(config.mbdir);
    std::string lock_file = "/tmp/_mbh_lock";
    if (directory_exists(db_dir)) {
        lock_file = db_dir + "/_mbh_lock";
    }

    int fd = acquire_file_lock_wait_n(lock_file, 5000);
    InitDBEx(config);
    if ((config.options & CONSTS::ACCESS_MODE_WRITER) && !is_open()
        && status != MBError::WRITER_EXIST && status != MBError::NOT_ALLOWED) {
        ReInit(config);
    }
    release_file_lock(fd);
}

void DB::InitDBEx(MBConfig& config)
{
    dict = NULL;
    async_writer = NULL;

    if (ValidateConfig(config) != MBError::SUCCESS)
        return;

    // save the configuration
    memcpy(&dbConfig, &config, sizeof(MBConfig));
    dbConfig.mbdir = NULL;

    // If id not given, use thread ID
    if (config.connect_id == 0) {
#ifdef __APPLE__
        config.connect_id = reinterpret_cast<uint64_t>(pthread_self()) & 0x7FFFFFFF;
#else
        config.connect_id = static_cast<uint32_t>(syscall(SYS_gettid));
#endif
    }
    identifier = config.connect_id;
    mb_dir = std::string(config.mbdir);
    if (mb_dir[mb_dir.length() - 1] != '/')
        mb_dir += "/";
    options = config.options;

    bool init_header = false;
    bool update_header = false; // true when header version is different from lib version
    PreCheckDB(config, init_header, update_header);
    if (MBError::NOT_INITIALIZED != status) {
        Logger::Log(LOG_LEVEL_ERROR, "database %s check failed: %s", mb_dir.c_str(),
            MBError::get_error_str(status));
        return;
    }

    try {
        dict = new Dict(mb_dir, init_header, config.data_size, config.options,
            config.memcap_index, config.memcap_data,
            config.block_size_index, config.block_size_data,
            config.max_num_index_block, config.max_num_data_block,
            config.num_entry_per_bucket, config.queue_size,
            config.queue_dir);
    } catch (int error) {
        status = error;
        Logger::Log(LOG_LEVEL_ERROR, "database %s check failed: %s", mb_dir.c_str(),
            MBError::get_error_str(status));
        if (!(config.options & CONSTS::ACCESS_MODE_WRITER))
            Logger::Log(LOG_LEVEL_WARN, "check if db writer is running.");
        return;
    }

    // Prefix cache: auto-enable only if DB was created with OPTION_PREFIX_CACHE
    // (embedded) or reader requested the option and cache can attach.

    PostDBUpdate(config, init_header, update_header);
}

int DB::Status() const
{
    return status;
}

DB::DB(const DB& db)
    : status(MBError::NOT_INITIALIZED)
    , rebuild_guard_fd(-1)
    , process_start_time(0)
    , reader_guard_fast_slot_count(0)
    , reader_guard_barrier_fallback_count(0)
    , writer_lock_fd(-1)
{
    MBConfig db_config = db.dbConfig;
    db_config.mbdir = db.mb_dir.c_str();
    db_config.options = CONSTS::ACCESS_MODE_READER;
    InitDB(db_config);
}

const DB& DB::operator=(const DB& db)
{
    if (this == &db)
        return *this; // no self-assignment

    this->Close();

    MBConfig db_config = db.dbConfig;
    db_config.mbdir = db.mb_dir.c_str();
    status = MBError::NOT_INITIALIZED;
    rebuild_guard_fd = -1;
    process_start_time = 0;
    reader_guard_fast_slot_count = 0;
    reader_guard_barrier_fallback_count = 0;
    writer_lock_fd = -1;
    InitDB(db_config);

    return *this;
}

bool DB::is_open() const
{
    return status == MBError::SUCCESS;
}

const char* DB::StatusStr() const
{
    return MBError::get_error_str(status);
}

// Check if key is in DB
bool DB::InDB(const char* key, int len, int& err)
{
    err = MBError::SUCCESS;
    if (key == nullptr || len == 0) {
        return false;
    }
    if (status != MBError::SUCCESS) {
        err = MBError::NOT_INITIALIZED;
        return false;
    }
    // Writer in async mode cannot be used for lookup
    if (options & CONSTS::ASYNC_WRITER_MODE) {
        err = MBError::NOT_ALLOWED;
        return false;
    }
    MBData data(0, CONSTS::OPTION_FIND_AND_STORE_PARENT);
    detail::SearchEngine engine(*dict);
    int rval = engine.find(reinterpret_cast<const uint8_t*>(key), len, data);
    if (rval == MBError::IN_DICT) {
        return true; // found it
    } else if (rval != MBError::NOT_EXIST) {
        err = rval; // error
    }
    return false;
}

// Find the exact key match (delegate to SearchEngine)
int DB::Find(const char* key, int len, MBData& mdata) const
{
    if (key == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if (options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    uint64_t reader_epoch = BeginReaderEpochGuard();
    detail::SearchEngine engine(*dict);
    int rval = engine.find(reinterpret_cast<const uint8_t*>(key), len, mdata);
    EndReaderEpochGuard(reader_epoch);
    return rval;
}

int DB::Find(const std::string& key, MBData& mdata) const
{
    return Find(key.data(), key.size(), mdata);
}

int DB::FindLowerBound(const std::string& key, MBData& data, std::string* bound_key) const
{
    return FindLowerBound(key.data(), key.size(), data, bound_key);
}

int DB::FindLowerBound(const char* key, int len, MBData& data, std::string* bound_key) const
{
    if (key == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if (options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    data.options = 0;
    if (bound_key != nullptr)
        bound_key->reserve(CONSTS::MAX_KEY_LENGHTH);
    detail::SearchEngine engine(*dict);
    return engine.lowerBound(reinterpret_cast<const uint8_t*>(key), len, data, bound_key);
}

// Find the longest prefix match
int DB::FindLongestPrefix(const char* key, int len, MBData& data) const
{
    if (key == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if (options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    data.match_len = 0;
    uint64_t reader_epoch = BeginReaderEpochGuard();
    detail::SearchEngine engine(*dict);
    int rval = engine.findPrefix(reinterpret_cast<const uint8_t*>(key), len, data);
    EndReaderEpochGuard(reader_epoch);
    return rval;
}

int DB::FindLongestPrefix(const std::string& key, MBData& data) const
{
    return FindLongestPrefix(key.data(), key.size(), data);
}

int DB::ReadDataByOffset(size_t offset, MBData& data) const
{
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    return dict->ReadDataByOffset(offset, data);
}

int DB::WriteDataByOffset(size_t offset, const char* data, int data_len) const
{
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    try {
        dict->WriteData(reinterpret_cast<const uint8_t*>(data), data_len, offset);
    } catch (int error) {
        return error;
    }
    return MBError::SUCCESS;
}

uint8_t* DB::GetDataPtrByOffset(size_t offset) const
{
    if (status != MBError::SUCCESS)
        return nullptr;
    return dict->GetShmPtr(offset, 0);
}

// Add a key-value pair
int DB::Add(const char* key, int len, MBData& mbdata, bool overwrite)
{
    int rval = MBError::SUCCESS;

    if (key == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if (async_writer == NULL && (options & CONSTS::ACCESS_MODE_WRITER)) {
        rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);
    } else {
        AsyncWriter* awr = AsyncWriter::GetInstance();
        if (awr) {
            try {
                rval = awr->AddWithLock(key, len, mbdata, overwrite);
                if (!overwrite && rval == MBError::IN_DICT)
                    rval = MBError::SUCCESS;
            } catch (int error) {
                rval = error;
            }
        } else {
            rval = MBError::TRY_AGAIN;
        }

        int retry_cnt = 0;
        while (rval == MBError::TRY_AGAIN) {
            rval = dict->SHMQ_Add(reinterpret_cast<const char*>(key), len,
                reinterpret_cast<const char*>(mbdata.buff), mbdata.data_len, overwrite);
            if (!(mbdata.options & CONSTS::OPTION_SHMQ_RETRY)) {
                break;
            }
            if (retry_cnt++ > MB_SHM_RETRY_TIMEOUT) {
                break;
            }
            usleep(1);
        }
    }

    return rval;
}

int DB::AddAsync(const char* key, int len, const char* data, int data_len, bool overwrite)
{
    MBData mbdata;
    mbdata.data_len = data_len;
    mbdata.buff = (uint8_t*)data;
    mbdata.options |= CONSTS::OPTION_SHMQ_RETRY;

    int rval = Add(key, len, mbdata, overwrite);
    mbdata.buff = NULL;
    return rval;
}

int DB::Add(const char* key, int len, const char* data, int data_len, bool overwrite)
{
    MBData mbdata;
    mbdata.data_len = data_len;
    mbdata.buff = (uint8_t*)data;

    int rval = Add(key, len, mbdata, overwrite);
    mbdata.buff = NULL;
    return rval;
}

int DB::Add(const std::string& key, const std::string& value, bool overwrite)
{
    return Add(key.data(), key.size(), value.data(), value.size(), overwrite);
}

int DB::Remove(const char* key, int len)
{
    int rval = MBError::SUCCESS;

    if (key == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if (async_writer == NULL && (options & CONSTS::ACCESS_MODE_WRITER)) {
        rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len);
    } else {
        rval = dict->SHMQ_Remove(reinterpret_cast<const char*>(key), len);
    }

    return rval;
}

int DB::RemoveAsync(const char* key, int len)
{
    if (key == nullptr || len == 0)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    int rval = MBError::SUCCESS;
    int retry_cnt = 0;
    do {
        rval = dict->SHMQ_Remove(reinterpret_cast<const char*>(key), len);
        if (rval != MBError::TRY_AGAIN || retry_cnt++ > MB_SHM_RETRY_TIMEOUT) {
            break;
        }
        usleep(1);
    } while (true);
    return rval;
}

int DB::Remove(const std::string& key)
{
    return Remove(key.data(), key.size());
}

int DB::RemoveAll()
{
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    int rval;
    if (async_writer == NULL && (options & CONSTS::ACCESS_MODE_WRITER)) {
        rval = dict->RemoveAll();
    } else {
        rval = dict->SHMQ_RemoveAll();
    }
    return rval;
}

int DB::RemoveAllSync()
{
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if (!(options & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    int rval = dict->RemoveAll();
    return rval;
}

int DB::Backup(const char* bk_dir)
{
    int rval = MBError::SUCCESS;

    if (options & CONSTS::MEMORY_ONLY_MODE)
        return MBError::NOT_ALLOWED;

    if (bk_dir == NULL)
        return MBError::INVALID_ARG;
    if (status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if (options & MMAP_ANONYMOUS_MODE)
        return MBError::NOT_ALLOWED;

    try {
        if (async_writer == NULL && (options & CONSTS::ASYNC_WRITER_MODE)) {
            DBBackup bk(*this);
            rval = bk.Backup(bk_dir);
        } else {
            rval = dict->SHMQ_Backup(bk_dir);
        }
    } catch (int error) {
        Logger::Log(LOG_LEVEL_WARN, "Backup failed :%s", MBError::get_error_str(error));
        rval = error;
    }

    return rval;
}

void DB::Flush() const
{
    if (options & CONSTS::MEMORY_ONLY_MODE)
        return;

    if (status != MBError::SUCCESS)
        return;

    dict->Flush();
}

void DB::Purge() const
{
    if (status != MBError::SUCCESS)
        return;

    dict->Purge();
}

// EnablePrefixCache/DisablePrefixCache APIs removed; prefix cache must be
// configured via CONSTS::OPTION_PREFIX_CACHE at DB creation.

void DB::DumpPrefixCacheStats(std::ostream& os) const
{
    if (dict)
        dict->PrintPrefixCacheStats(os);
}

int DB::CollectResource(int64_t min_index_rc_size, int64_t min_data_rc_size,
    int64_t max_dbsz, int64_t max_dbcnt)
{
    if (status != MBError::SUCCESS)
        return status;

    try {
        if (async_writer == NULL && (options & CONSTS::ACCESS_MODE_WRITER)) {
            ResourceCollection rc(*this);
            rc.ReclaimResource(min_index_rc_size, min_data_rc_size, max_dbsz, max_dbcnt);
        } else {
            dict->SHMQ_CollectResource(min_index_rc_size, min_data_rc_size, max_dbsz, max_dbcnt);
        }
    } catch (int error) {
        if (error != MBError::RC_SKIPPED) {
            Logger::Log(LOG_LEVEL_ERROR, "failed to run gc: %s",
                MBError::get_error_str(error));
            return error;
        }
    }

    return MBError::SUCCESS;
}

int64_t DB::Count() const
{
    if (status != MBError::SUCCESS)
        return -1;

    return dict->Count();
}

int64_t DB::GetPendingDataBufferSize() const
{
    if (status != MBError::SUCCESS)
        return -1;

    return dict->GetHeaderPtr()->pending_data_buff_size;
}

int64_t DB::GetPendingIndexBufferSize() const
{
    if (status != MBError::SUCCESS)
        return -1;
    return dict->GetHeaderPtr()->pending_index_buff_size;
}

void DB::PrintStats(std::ostream& out_stream) const
{
    if (status != MBError::SUCCESS)
        return;

    dict->PrintStats(out_stream);
}

void DB::PrintHeader(std::ostream& out_stream) const
{
    if (dict != NULL)
        dict->PrintHeader(out_stream);
}

int DB::Lock()
{
    return lock.Lock();
}

int DB::UnLock()
{
    return lock.UnLock();
}

int DB::ClearLock() const
{
    // No db handler should hold mutex when this is called.
    if (status != MBError::SUCCESS)
        return status;
    return InitShmMutex(dict->GetShmLockPtr());
}

int DB::SetLogLevel(int level)
{
    return Logger::SetLogLevel(level);
}

void DB::LogDebug()
{
    Logger::SetLogLevel(LOG_LEVEL_DEBUG);
}

Dict* DB::GetDictPtr() const
{
    // Only allow writer to access dict directly
    if (options & CONSTS::ACCESS_MODE_WRITER)
        return dict;
    return NULL;
}

int DB::GetDBOptions() const
{
    return options;
}

const std::string& DB::GetDBDir() const
{
    return mb_dir;
}

void DB::GetDBConfig(MBConfig& config) const
{
    memcpy(&config, &dbConfig, sizeof(MBConfig));
    config.mbdir = NULL;
}

bool DB::AsyncWriterEnabled() const
{
    return true;
}

bool DB::AsyncWriterBusy() const
{
    return dict->SHMQ_Busy();
}

void DB::SetLogFile(const std::string& log_file)
{
    Logger::InitLogFile(log_file);
}

void DB::CloseLogFile()
{
    Logger::Close();
}

void DB::ClearResources(const std::string& path)
{
    ResourcePool::getInstance().RemoveResourceByDB(path);
}

int DB::GetDataHeaderSize()
{
    return DATA_HDR_BYTE;
}

} // namespace mabain
