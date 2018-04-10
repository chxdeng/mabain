/*
 * Copyright (C) 2017 Cisco Inc.
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

#include <iostream>
#include <unistd.h>
#include <sys/syscall.h>

#include <errno.h>

#include "dict.h"
#include "db.h"
#include "error.h"
#include "logger.h"
#include "mb_lsq.h"
#include "version.h"
#include "mb_rc.h"
#include "integer_4b_5b.h"
#include "async_writer.h"
#include "mb_backup.h"
#include "resource_pool.h"

namespace mabain {

// Current mabain version 1.1.0
uint16_t version[4] = {1, 1, 0, 0};

DB::~DB()
{
}

int DB::Close()
{
    int rval = MBError::SUCCESS;

    if((options & CONSTS::ACCESS_MODE_WRITER) && async_writer != NULL)
    {
        rval = async_writer->StopAsyncThread();
        if(rval != MBError::SUCCESS)
            return rval;

        delete async_writer;
        async_writer = NULL;
    }

    if(dict != NULL)
    {
        if(options & CONSTS::ACCESS_MODE_WRITER)
            dict->PrintStats(Logger::GetLogStream());
        UpdateNumHandlers(options, -1);

        dict->Destroy();
        delete dict;
        dict = NULL;
    }
    else
    {
        rval = status;
    }

    status = MBError::DB_CLOSED;
    Logger::Log(LOG_LEVEL_INFO, "connector %u disconnected from DB", identifier);
    return rval;
}

int DB::UpdateNumHandlers(int mode, int delta)
{
    int rval = MBError::SUCCESS;

    WrLock();

    if(mode & CONSTS::ACCESS_MODE_WRITER)
        rval = dict->UpdateNumWriter(delta);
    else
        dict->UpdateNumReader(delta);

    UnLock();

    return rval;
}

// Constructor for initializing DB handle
DB::DB(const char *db_path,
       int db_options,
       size_t memcap_index,
       size_t memcap_data,
       uint32_t id) : status(MBError::NOT_INITIALIZED)
{
    MBConfig config;
    memset(&config, 0, sizeof(config));
    config.mbdir = db_path;
    config.options = db_options;
    config.memcap_index = memcap_index;
    config.memcap_data = memcap_data;
    config.connect_id = id;

    InitDB(config);
}

DB::DB(MBConfig &config) : status(MBError::NOT_INITIALIZED)
{
    InitDB(config);
}

int DB::ValidateConfig(MBConfig &config)
{
    if(config.mbdir == NULL)
        return MBError::INVALID_ARG;

    if(config.memcap_index == 0)
        config.memcap_index = 2*config.block_size_index;
    if(config.memcap_data == 0)
        config.memcap_data = 2*config.block_size_data;

    if(config.options & CONSTS::ACCESS_MODE_WRITER)
    {
        if(config.block_size_index == 0)
            config.block_size_index = INDEX_BLOCK_SIZE_DEFAULT;
        if(config.block_size_data == 0)
            config.block_size_data = DATA_BLOCK_SIZE_DEFAULT;
        if(config.num_entry_per_bucket <= 0)
            config.num_entry_per_bucket = 1000;
        if(config.num_entry_per_bucket < 8)
        {
            std::cerr << "count in eviction bucket must be greater than 7\n";
            return MBError::INVALID_ARG;
        }
    }

    if(config.block_size_index != 0 && (config.block_size_index % BLOCK_SIZE_ALIGN != 0))
    {
        std::cerr << "block size must be multiple of " << BLOCK_SIZE_ALIGN << "\n";
        return MBError::INVALID_ARG;
    }
    if(config.block_size_data != 0 && (config.block_size_data % BLOCK_SIZE_ALIGN != 0))
    {
        std::cerr << "block size must be multiple of " << BLOCK_SIZE_ALIGN << "\n";
        return MBError::INVALID_ARG;
    }

    if(config.max_num_index_block == 0)
        config.max_num_index_block = 1024;
    if(config.max_num_data_block == 0)
        config.max_num_data_block = 1024;

    return MBError::SUCCESS;
}

void DB::InitDB(MBConfig &config)
{
    dict = NULL;
    async_writer = NULL;

    if(ValidateConfig(config) != MBError::SUCCESS)
        return;

    // save the configuration
    memcpy(&dbConfig, &config, sizeof(MBConfig));
    dbConfig.mbdir = NULL;

    // If id not given, use thread ID
    if(config.connect_id == 0)
        config.connect_id = static_cast<uint32_t>(syscall(SYS_gettid));
    identifier = config.connect_id;
    mb_dir = std::string(config.mbdir);
    if(mb_dir[mb_dir.length()-1] != '/')
        mb_dir += "/";
    options = config.options;

    bool init_header = false;
    if(config.options & CONSTS::MEMORY_ONLY_MODE)
    {
        if(config.options & CONSTS::ACCESS_MODE_WRITER)
        {
            init_header = true;
        }
        else
        {
            init_header = false;
            if(!ResourcePool::getInstance().CheckExistence(mb_dir + "_mabain_h"))
                status = MBError::NO_DB;
        }
    }
    else
    {
        // Check if the DB directory exist with proper permission
        if(access(mb_dir.c_str(), F_OK))
        {
            std::cerr << "database directory check for " + mb_dir + " failed errno: " +
                          std::to_string(errno) << std::endl;
            status = MBError::NO_DB;
            return;
        }
        Logger::Log(LOG_LEVEL_INFO, "connector %u DB options: %d",
                    config.connect_id, config.options);
        // Check if DB exist. This can be done by check existence of the first index file.
        // If this is the first time the DB is opened and it is in writer mode, then we
        // need to update the header for the first time. If only reader access mode is
        // required and the file does not exist, we should bail here and the DB open will
        // not be successful.
        std::string header_file = mb_dir + "_mabain_h";
        if(access(header_file.c_str(), R_OK))
        {
            if(config.options & CONSTS::ACCESS_MODE_WRITER)
                init_header = true;
            else
                status = MBError::NO_DB;
        }
    }
    if(status == MBError::NO_DB)
    {
        Logger::Log(LOG_LEVEL_ERROR, "database " + mb_dir + ": check failed");
        return;
    }

    dict = new Dict(mb_dir, init_header, config.data_size, config.options,
                    config.memcap_index, config.memcap_data,
                    config.block_size_index, config.block_size_data,
                    config.max_num_index_block, config.max_num_data_block,
                    config.num_entry_per_bucket);

    if((config.options & CONSTS::ACCESS_MODE_WRITER) && init_header)
    {
        Logger::Log(LOG_LEVEL_INFO, "open a new db %s", mb_dir.c_str());
        dict->Init(identifier);
#ifdef __SHM_LOCK__
        dict->InitShmMutex();
#endif
    }

    if(dict->Status() != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to iniitialize dict: %s ",
                    MBError::get_error_str(dict->Status()));
        return;
    }

    lock.Init(dict->GetShmLockPtrs());
    status = UpdateNumHandlers(config.options, 1);
    if(status != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to initialize db: %s",
                    MBError::get_error_str(status));
        return;
    }

    if(config.options & CONSTS::ACCESS_MODE_WRITER)
    {
        if(config.options & CONSTS::ASYNC_WRITER_MODE)
            async_writer = new AsyncWriter(this);
    }

    Logger::Log(LOG_LEVEL_INFO, "connector %u successfully opened DB %s for %s",
                identifier, mb_dir.c_str(),
                (config.options & CONSTS::ACCESS_MODE_WRITER) ? "writing":"reading");
    status = MBError::SUCCESS;

    // Run rc exception recovery
    ResourceCollection rc(*this);
    rc.ExceptionRecovery();
}

int DB::Status() const
{
    return status;
}

bool DB::is_open() const
{
    return status == MBError::SUCCESS;
}

const char* DB::StatusStr() const
{
    return MBError::get_error_str(status);
}

// Find the exact key match
int DB::Find(const char* key, int len, MBData &mdata) const
{
    if(key == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    // Writer in async mode cannot be used for lookup
    if(options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    return dict->Find(reinterpret_cast<const uint8_t*>(key), len, mdata);
}

int DB::Find(const std::string &key, MBData &mdata) const
{
    return Find(key.data(), key.size(), mdata);
}

// Find all possible prefix matches. The caller needs to call this function
// repeatedly if data.next is true.
int DB::FindPrefix(const char* key, int len, MBData &data) const
{
    if(key == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    // Writer in async mode cannot be used for lookup
    if(options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    if(data.match_len >= len)
        return MBError::OUT_OF_BOUND;

    int rval;
    rval = dict->FindPrefix(reinterpret_cast<const uint8_t*>(key+data.match_len),
                            len-data.match_len, data);

    return rval;
}

// Find the longest prefix match
int DB::FindLongestPrefix(const char* key, int len, MBData &data) const
{
    if(key == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    // Writer in async mode cannot be used for lookup
    if(options & CONSTS::ASYNC_WRITER_MODE)
        return MBError::NOT_ALLOWED;

    data.match_len = 0;

    return dict->FindPrefix(reinterpret_cast<const uint8_t*>(key), len, data);
}

int DB::FindLongestPrefix(const std::string &key, MBData &data) const
{
    return FindLongestPrefix(key.data(), key.size(), data);
}

// Add a key-value pair
int DB::Add(const char* key, int len, MBData &mbdata, bool overwrite)
{
    if(key == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Add(key, len, reinterpret_cast<const char *>(mbdata.buff),
                                 mbdata.data_len, overwrite);

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    return rval;
}

int DB::Add(const char* key, int len, const char* data, int data_len, bool overwrite)
{
    if(key == NULL || data == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Add(key, len, data, data_len, overwrite);

    MBData mbdata;
    mbdata.data_len = data_len;
    mbdata.buff = (uint8_t*) data;

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    mbdata.buff = NULL;
    return rval;
}

int DB::Add(const std::string &key, const std::string &value, bool overwrite)
{
    return Add(key.data(), key.size(), value.data(), value.size(), overwrite);
}

int DB::Remove(const char *key, int len)
{
    if(key == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Remove(key, len);

    int rval;
    rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len);

    return rval;
}

int DB::Remove(const std::string &key)
{
    return Remove(key.data(), key.size());
}

int DB::RemoveAll()
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->RemoveAll();

    int rval;
    rval = dict->RemoveAll();
    return rval;
}

int DB::Backup(const char *bk_dir)
{
    if(bk_dir == NULL)
        return MBError::INVALID_ARG;
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;
    if(options & MMAP_ANONYMOUS_MODE)
        return MBError::NOT_ALLOWED;

    if(async_writer != NULL)
        return async_writer->Backup(bk_dir);

    int rval;
    try {
        DBBackup bk(*this);
        rval = bk.Backup(bk_dir);
    } catch  (int error) {
        Logger::Log(LOG_LEVEL_WARN, "Backup failed :%s", MBError::get_error_str(error));
        rval = error;
    }
    return rval;
}

void DB::Flush() const
{
    if(status != MBError::SUCCESS)
        return;
    dict->Flush();
}

int DB::CollectResource(int64_t min_index_rc_size, int64_t min_data_rc_size,
                        int64_t max_dbsz, int64_t max_dbcnt)
{
    if(status != MBError::SUCCESS)
        return status;

    if(async_writer != NULL)
        return async_writer->CollectResource(min_index_rc_size, min_data_rc_size,
                                             max_dbsz, max_dbcnt);

    try {
        ResourceCollection rc(*this);
        rc.ReclaimResource(min_index_rc_size, min_data_rc_size, max_dbsz, max_dbcnt);
    } catch (int error) {
        if(error != MBError::RC_SKIPPED)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to run gc: %s",
                        MBError::get_error_str(error));
        }
        return error;
    }
    return MBError::SUCCESS;
}

int64_t DB::Count() const
{
    if(status != MBError::SUCCESS)
        return -1;

    return dict->Count();
}

void DB::PrintStats(std::ostream &out_stream) const
{
    if(status != MBError::SUCCESS)
        return;

    dict->PrintStats(out_stream);
}

void DB::PrintHeader(std::ostream &out_stream) const
{
    if(dict != NULL)
        dict->PrintHeader(out_stream);
}

int DB::WrLock()
{
    return lock.WrLock();
}

int DB::RdLock()
{
    return lock.RdLock();
}

int DB::UnLock()
{
    return lock.UnLock();
}

int DB::TryWrLock()
{
    return lock.TryWrLock();
}

int DB::ClearLock() const
{
#ifdef __SHM_LOCK__
    // No db handler should hold mutex when this is called.
    return dict->InitShmMutex();
#else
    // Nothing needs to be done if we don't use shared memory mutex.
    return MBError::SUCCESS;
#endif
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
    if(options & CONSTS::ACCESS_MODE_WRITER)
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

void DB::GetDBConfig(MBConfig &config) const
{
    memcpy(&config, &dbConfig, sizeof(MBConfig));
    config.mbdir = NULL;
}

int DB::SetAsyncWriterPtr(DB *db_writer)
{
    if(db_writer == NULL)
        return MBError::INVALID_ARG;
    if(options & CONSTS::ACCESS_MODE_WRITER)
        return MBError::NOT_ALLOWED;
    if(db_writer->mb_dir != mb_dir)
        return MBError::INVALID_ARG;
    if(!(db_writer->options & CONSTS::ACCESS_MODE_WRITER) ||
       !(db_writer->options & CONSTS::ASYNC_WRITER_MODE)  ||
       db_writer->async_writer == NULL)
    {
        return MBError::INVALID_ARG;
    }

   db_writer->async_writer->UpdateNumUsers(1);
   async_writer = db_writer->async_writer;
   return MBError::SUCCESS;
}

int DB::UnsetAsyncWriterPtr(DB *db_writer)
{
    if(db_writer == NULL)
        return MBError::INVALID_ARG;
    if(options & CONSTS::ACCESS_MODE_WRITER)
        return MBError::NOT_ALLOWED;
    if(db_writer->mb_dir != mb_dir)
        return MBError::INVALID_ARG;
    if(!(db_writer->options & CONSTS::ACCESS_MODE_WRITER) ||
       !(db_writer->options & CONSTS::ASYNC_WRITER_MODE)  ||
       db_writer->async_writer == NULL)
    {
        return MBError::INVALID_ARG;
    }

    db_writer->async_writer->UpdateNumUsers(-1);
    async_writer = NULL;
    return MBError::SUCCESS;
}

bool DB::AsyncWriterEnabled() const
{
    return (async_writer != NULL);
}

bool DB::AsyncWriterBusy() const
{
    if(async_writer != NULL)
        return async_writer->Busy();
    return false;
}

void DB::SetLogFile(const std::string &log_file)
{
    Logger::InitLogFile(log_file);
}

void DB::CloseLogFile()
{
    Logger::Close();
}

void DB::ClearResources(const std::string &path)
{
    ResourcePool::getInstance().RemoveResourceByDB(path);
}

} // namespace mabain
