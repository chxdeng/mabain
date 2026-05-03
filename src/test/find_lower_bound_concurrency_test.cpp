#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <new>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "../db.h"
#include "../error.h"

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

using namespace mabain;

namespace {

struct SharedState {
    std::atomic<int> start { 0 };
    std::atomic<int> stop { 0 };
};

struct TestConfig {
    std::string db_dir = "/tmp/mabain_find_lower_bound_concurrency";
    int reader_count = 24;
    int key_count = 200000;
    int writer_ops = 5000000;
    int anchor_count = 16;
};

struct ReturnStats {
    uint64_t success = 0;
    uint64_t not_exist = 0;
    uint64_t try_again = 0;
    uint64_t other = 0;
};

std::string HotKey(int i)
{
    char buff[64];
    std::snprintf(buff, sizeof(buff), "flb_hot_%08d", i);
    return std::string(buff);
}

std::string AnchorKey(int i)
{
    char buff[64];
    std::snprintf(buff, sizeof(buff), "flb_anchor_%08d", i);
    return std::string(buff);
}

std::string ValueForKey(const std::string& key)
{
    return "value:" + key;
}

std::vector<std::string> BuildQueryKeys(int key_count)
{
    std::vector<std::string> keys;
    keys.reserve(key_count);
    for (int i = 0; i < key_count; ++i)
        keys.push_back(HotKey(i));
    return keys;
}

void WaitForStart(SharedState* state)
{
    while (!state->start.load(std::memory_order_acquire))
        usleep(1000);
}

void AbortOnError(const std::string& message)
{
    std::cerr << message << std::endl;
    abort();
}

void CleanDbDir(const std::string& db_dir)
{
    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    std::filesystem::create_directories(db_dir, ec);
    if (ec)
        AbortOnError("failed to prepare db dir: " + db_dir + ": " + ec.message());
}

void AddOrAbort(DB& db, const std::string& key)
{
    const std::string value = ValueForKey(key);
    int rc = db.Add(key, value, true);
    if (rc != MBError::SUCCESS)
        AbortOnError("Add failed for " + key + ": " + MBError::get_error_str(rc));
}

void SeedDbOrAbort(const TestConfig& cfg, const std::vector<std::string>& keys)
{
    DB db(cfg.db_dir.c_str(), CONSTS::WriterOptions());
    if (!db.is_open())
        AbortOnError("writer seed open failed: " + std::string(db.StatusStr()));

    for (int i = 0; i < cfg.anchor_count; ++i)
        AddOrAbort(db, AnchorKey(i));
    for (const std::string& key : keys)
        AddOrAbort(db, key);

    db.Close();
}

int WriterMain(const TestConfig& cfg, SharedState* state)
{
    WaitForStart(state);

    DB db(cfg.db_dir.c_str(), CONSTS::WriterOptions());
    if (!db.is_open()) {
        std::cerr << "writer open failed: " << db.StatusStr() << std::endl;
        return 2;
    }

    for (int op = 0; op < cfg.writer_ops && !state->stop.load(std::memory_order_acquire); ++op) {
        int index = op % cfg.key_count;
        const std::string key = HotKey(index);
        int rc;
        if (((op / cfg.key_count) & 1) == 0) {
            rc = db.Remove(key);
            if (rc != MBError::SUCCESS && rc != MBError::NOT_EXIST) {
                std::cerr << "Remove failed for " << key << ": " << MBError::get_error_str(rc) << std::endl;
                return 3;
            }
        } else {
            const std::string value = ValueForKey(key);
            rc = db.Add(key, value, true);
            if (rc != MBError::SUCCESS) {
                std::cerr << "Add failed for " << key << ": " << MBError::get_error_str(rc) << std::endl;
                return 4;
            }
        }
    }

    db.Close();
    return 0;
}

void ValidateLowerBoundResult(const std::string& query, const std::string& bound_key, const MBData& data)
{
    if (bound_key.empty())
        AbortOnError("FindLowerBound returned SUCCESS with empty bound key for query " + query);
    if (bound_key > query)
        AbortOnError("FindLowerBound returned key greater than query: query=" + query + " bound=" + bound_key);
    if (data.buff == nullptr || data.data_len <= 0)
        AbortOnError("FindLowerBound returned SUCCESS with empty value for query " + query + " bound=" + bound_key);

    const std::string expected = ValueForKey(bound_key);
    const std::string actual(reinterpret_cast<const char*>(data.buff), data.data_len);
    if (actual != expected) {
        std::ostringstream msg;
        msg << "FindLowerBound value mismatch: query=" << query
            << " bound=" << bound_key
            << " expected=" << expected
            << " actual=" << actual;
        AbortOnError(msg.str());
    }
}

void CountReturnValue(ReturnStats& stats, int rc)
{
    if (rc == MBError::SUCCESS)
        stats.success++;
    else if (rc == MBError::NOT_EXIST)
        stats.not_exist++;
    else if (rc == MBError::TRY_AGAIN)
        stats.try_again++;
    else
        stats.other++;
}

void PrintReturnStats(int reader_index, const ReturnStats& stats)
{
    uint64_t total = stats.success + stats.not_exist + stats.try_again + stats.other;
    double success_pct = total == 0 ? 0.0 : (100.0 * stats.success) / total;

    std::cout << "reader " << reader_index << " FindLowerBound return stats:"
              << " total=" << total
              << " success=" << stats.success
              << " not_exist=" << stats.not_exist
              << " try_again=" << stats.try_again
              << " other=" << stats.other
              << " success_pct=" << std::fixed << std::setprecision(2)
              << success_pct << "%"
              << std::endl;
}

int ReaderMain(const TestConfig& cfg, const std::vector<std::string>& queries, SharedState* state, int reader_index)
{
    WaitForStart(state);

    DB db(cfg.db_dir.c_str(), CONSTS::ReaderOptions());
    if (!db.is_open()) {
        std::cerr << "reader " << reader_index << " open failed: " << db.StatusStr() << std::endl;
        return 5;
    }

    MBData data;
    size_t pos = static_cast<size_t>(reader_index) % queries.size();
    ReturnStats stats;
    while (!state->stop.load(std::memory_order_acquire)) {
        const std::string& query = queries[pos];
        std::string bound_key;
        int rc = db.FindLowerBound(query, data, &bound_key);
        CountReturnValue(stats, rc);
        if (rc == MBError::SUCCESS) {
            ValidateLowerBoundResult(query, bound_key, data);
        } else if (rc != MBError::NOT_EXIST && rc != MBError::TRY_AGAIN) {
            AbortOnError("FindLowerBound failed for " + query + ": " + MBError::get_error_str(rc));
        }

        pos++;
        if (pos == queries.size())
            pos = 0;
    }

    if (reader_index == 0)
        PrintReturnStats(reader_index, stats);

    db.Close();
    return 0;
}

SharedState* CreateSharedState()
{
    void* mem = mmap(nullptr, sizeof(SharedState), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED)
        AbortOnError("mmap shared state failed");
    return new (mem) SharedState();
}

bool WaitForProcess(pid_t pid, const char* role)
{
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        std::cerr << "waitpid failed for " << role << std::endl;
        return false;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
        return true;
    if (WIFSIGNALED(status)) {
        std::cerr << role << " exited by signal " << WTERMSIG(status) << std::endl;
    } else if (WIFEXITED(status)) {
        std::cerr << role << " exited with status " << WEXITSTATUS(status) << std::endl;
    }
    return false;
}

int ParsePositiveArg(char** argv, int argc, int index, int fallback)
{
    if (argc <= index)
        return fallback;
    int value = std::atoi(argv[index]);
    return value > 0 ? value : fallback;
}

} // namespace

int main(int argc, char** argv)
{
    TestConfig cfg;
    if (argc > 1)
        cfg.db_dir = argv[1];
    cfg.reader_count = ParsePositiveArg(argv, argc, 2, cfg.reader_count);
    cfg.key_count = ParsePositiveArg(argv, argc, 3, cfg.key_count);
    cfg.writer_ops = ParsePositiveArg(argv, argc, 4, cfg.writer_ops);

    CleanDbDir(cfg.db_dir);
    std::vector<std::string> queries = BuildQueryKeys(cfg.key_count);
    SeedDbOrAbort(cfg, queries);

    SharedState* state = CreateSharedState();
    std::vector<pid_t> readers;
    readers.reserve(cfg.reader_count);

    pid_t writer_pid = fork();
    if (writer_pid < 0)
        AbortOnError("failed to fork writer");
    if (writer_pid == 0)
        return WriterMain(cfg, state);

    for (int i = 0; i < cfg.reader_count; ++i) {
        pid_t pid = fork();
        if (pid < 0)
            AbortOnError("failed to fork reader");
        if (pid == 0)
            return ReaderMain(cfg, queries, state, i);
        readers.push_back(pid);
    }

    state->start.store(1, std::memory_order_release);
    bool passed = WaitForProcess(writer_pid, "writer");
    state->stop.store(1, std::memory_order_release);

    for (pid_t reader_pid : readers)
        passed = WaitForProcess(reader_pid, "reader") && passed;

    munmap(state, sizeof(SharedState));

    if (!passed)
        return 1;

    std::cout << "find_lower_bound_concurrency_test passed"
              << " readers=" << cfg.reader_count
              << " keys=" << cfg.key_count
              << " writer_ops=" << cfg.writer_ops
              << std::endl;
    return 0;
}
