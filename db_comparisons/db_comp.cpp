#include <assert.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <sstream>
#include <string>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#ifdef LEVEL_DB
#include <leveldb/db.h>
#elif KYOTO_CABINET
#include <kchashdb.h>
#include <kcpolydb.h>
#elif LMDB
extern "C" {
#include <lmdb.h>
}
#elif MABAIN
#include <mabain/db.h>
#endif

#ifdef LEVEL_DB
leveldb::DB* db = NULL;
#elif KYOTO_CABINET
kyotocabinet::HashDB* db = NULL;
#elif LMDB
MDB_env* env = NULL;
MDB_dbi db;
MDB_txn* txn = NULL;
#elif MABAIN
mabain::DB* db = NULL;
#endif

#define ONE_MILLION 1000000

static const char* db_dir = "/var/tmp/db_test/";
static int num_kv = 1 * ONE_MILLION;
static int n_reader = 7;
static int key_type = 0;
static bool sync_on_write = false;
static unsigned long long memcap = 1024ULL * 1024 * 1024;
static bool use_jemalloc = false;
// Prefix cache controls (MABAIN only)
static long long pc_cap = -1; // capacity; -1 means default
// Shared prefix cache controls (enable via -pcc)

// Buffer per-reader cache stats to avoid interleaved output
static std::vector<std::string> g_reader_stats;
static pthread_mutex_t g_stats_mutex = PTHREAD_MUTEX_INITIALIZER;

// Deterministic pseudo-random generator mapping index->uint32_t
// Uses splitmix64 scramble and returns lower 32 bits. Stable across runs.
static inline uint32_t prand_u32(uint64_t i)
{
    uint64_t x = i + 0x9e3779b97f4a7c15ULL; // golden ratio seed
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    // Return modulo pcc (pc_cap) when provided, otherwise full 32-bit value
    // if (pc_cap > 0)
    //    return static_cast<uint32_t>(x % static_cast<uint64_t>(pc_cap));
    return static_cast<uint32_t>(x);
}
static inline uint64_t prand_u64(uint64_t i)
{
    uint64_t x = i + 0x9e3779b97f4a7c15ULL; // golden ratio seed
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static void get_sha256_str(int key, char* sha256_str)
{
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        sha256_str[0] = 0;
        return;
    }

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 || EVP_DigestUpdate(ctx, (unsigned char*)&key, 4) != 1 || EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
        EVP_MD_CTX_free(ctx);
        sha256_str[0] = 0;
        return;
    }

    EVP_MD_CTX_free(ctx);

    for (unsigned int i = 0; i < hash_len; i++) {
        sprintf(sha256_str + (i * 2), "%02x", hash[i]);
    }
    sha256_str[hash_len * 2] = 0;
}

static void get_sha1_str(int key, char* sha1_str)
{
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        sha1_str[0] = 0;
        return;
    }

    if (EVP_DigestInit_ex(ctx, EVP_sha1(), NULL) != 1 || EVP_DigestUpdate(ctx, (unsigned char*)&key, 4) != 1 || EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
        EVP_MD_CTX_free(ctx);
        sha1_str[0] = 0;
        return;
    }

    EVP_MD_CTX_free(ctx);

    for (unsigned int i = 0; i < hash_len; i++) {
        sprintf(sha1_str + (i * 2), "%02x", hash[i]);
    }
    sha1_str[hash_len * 2] = 0;
}

static void print_cpu_info()
{
    std::ifstream cpu_info("/proc/cpuinfo", std::fstream::in);
    if (!cpu_info.is_open()) {
        return;
    }

    std::string line;
    std::string model_name;
    std::string vendor_id;
    std::string cpu_freq;
    int n_processor = 0;
    while (std::getline(cpu_info, line)) {
        if (line.find("model name") != std::string::npos) {
            model_name = line;
            n_processor++;
        } else if (line.find("vendor_id") != std::string::npos) {
            vendor_id = line;
        } else if (line.find("cpu MHz") != std::string::npos) {
            cpu_freq = line;
        }
    }
    cpu_info.close();

    std::cout << "number of CPUs\t: " << n_processor << "\n";
    std::cout << vendor_id << "\n";
    std::cout << model_name << "\n";
    std::cout << cpu_freq << "\n";
}

static void InitTestDir()
{
    std::string cmd;

    cmd = std::string("mkdir -p ") + db_dir;
    if (system(cmd.c_str()) != 0) {
    }

    std::string db_dir_tmp;
#ifdef LEVEL_DB
    std::cout << "===== using leveldb for testing\n";
    db_dir_tmp = std::string(db_dir) + "/leveldb/";
#elif KYOTO_CABINET
    std::cout << "===== using kyotocabinet for testing\n";
    db_dir_tmp = std::string(db_dir) + "/kyotocabinet/";
#elif LMDB
    std::cout << "===== using lmdb for testing\n";
    db_dir_tmp = std::string(db_dir) + "/lmdb/";
#elif MABAIN
    std::cout << "===== using mabain for testing\n";
    db_dir_tmp = std::string(db_dir) + "/mabain/";
#endif
    cmd = std::string("mkdir -p ") + db_dir_tmp;
    if (system(cmd.c_str()) != 0) {
    }
    cmd = std::string("rm -rf ") + db_dir_tmp + "*";
    if (system(cmd.c_str()) != 0) {
    }
}

static void InitDB(bool writer_mode = true)
{
#ifdef LEVEL_DB
    std::string db_dir_tmp = std::string(db_dir) + "/leveldb/";
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_dir_tmp, &db);
    assert(status.ok());
#elif KYOTO_CABINET
    std::string db_file = std::string(db_dir) + "/kyotocabinet/dbbench_hashDB.kch";
    db = new kyotocabinet::HashDB();
    int open_options = kyotocabinet::PolyDB::OWRITER | kyotocabinet::PolyDB::OCREATE;
    db->tune_map(memcap);
    if (!db->open(db_file, open_options)) {
        fprintf(stderr, "open error: %s\n", db->error().name());
    }
#elif LMDB
    std::string db_dir_tmp = std::string(db_dir) + "/lmdb";
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, memcap);
    mdb_env_open(env, db_dir_tmp.c_str(), 0, 0664);
    mdb_txn_begin(env, NULL, 0, &txn);
    mdb_open(txn, NULL, 0, &db);
    mdb_txn_commit(txn);
#elif MABAIN
    mabain::MBConfig mconf;
    memset(&mconf, 0, sizeof(mconf));
    std::string mbdir_tmp = std::string(db_dir) + "/mabain/";
    mconf.mbdir = mbdir_tmp.c_str();
    mconf.options = mabain::CONSTS::WriterOptions();
    if (use_jemalloc) {
        // In jemalloc mode, memcap must equal block_size * max_num_blocks
        mconf.options |= mabain::CONSTS::OPTION_JEMALLOC;
        mconf.block_size_index = 128LL * 1024 * 1024;
        mconf.block_size_data = 128LL * 1024 * 1024;
        mconf.max_num_index_block = 10;
        mconf.max_num_data_block = 10;
        mconf.memcap_index = mconf.block_size_index * mconf.max_num_index_block;
        mconf.memcap_data = mconf.block_size_data * mconf.max_num_data_block;
    } else {
        mconf.block_size_index = 67LL * 1024 * 1024;
        mconf.block_size_data = 67LL * 1024 * 1024;
        mconf.memcap_index = (unsigned long long)(0.6666667 * memcap);
        mconf.memcap_data = (unsigned long long)(0.3333333 * memcap);
        mconf.max_num_index_block = 10;
        mconf.max_num_data_block = 10;
    }
    if (sync_on_write) {
        mconf.options |= mabain::CONSTS::SYNC_ON_WRITE;
    }
    if (!writer_mode)
        mconf.options = mabain::CONSTS::ReaderOptions();
    db = new mabain::DB(mconf);
    // Ensure DB is open; subsequent phases depend on it
    assert(db->is_open());
    // Enable shared prefix cache when -pcc is provided
    if (pc_cap > 0) {
        db->EnableSharedPrefixCache(static_cast<size_t>(pc_cap));
    }
#endif
}

static void Add(int n)
{
    char kv[65];

#if LMDB
    if (!sync_on_write)
        mdb_txn_begin(env, NULL, 0, &txn);
#endif

    uint64_t total_time = 0;

    for (int i = 0; i < n; i++) {
        std::string key, val;
        if (key_type == 0) {
            key = std::to_string(i);
            val = std::to_string(i);
        } else if (key_type == 1 || key_type == 2) {
            if (key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
            val = kv;
        } else { // key_type == u32 pseudo-random (binary 3 bytes + decimal suffix)
            uint32_t r = prand_u64(static_cast<uint64_t>(i));
            key.assign((char*)&r, 4);
            key += std::to_string(i);
            val = key;
        }

#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        auto start = std::chrono::high_resolution_clock::now();
        db->Put(opts, key, val);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
#elif KYOTO_CABINET
        auto start = std::chrono::high_resolution_clock::now();
        db->set(key.data(), key.size(), val.data(), val.size());
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
#elif LMDB
        MDB_val lmdb_key, lmdb_val;
        lmdb_key.mv_size = key.size();
        lmdb_key.mv_data = (void*)key.data();
        lmdb_val.mv_size = val.size();
        lmdb_val.mv_data = (void*)val.data();
        MDB_cursor* mc;
        if (sync_on_write) {
            mdb_txn_begin(env, NULL, 0, &txn);
            mdb_cursor_open(txn, db, &mc);
            auto start = std::chrono::high_resolution_clock::now();
            mdb_cursor_put(mc, &lmdb_key, &lmdb_val, 0);
            auto stop = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
            mdb_cursor_close(mc);
            mdb_txn_commit(txn);
        } else {
            mdb_cursor_open(txn, db, &mc);
            auto start = std::chrono::high_resolution_clock::now();
            mdb_cursor_put(mc, &lmdb_key, &lmdb_val, 0);
            auto stop = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
            mdb_cursor_close(mc);
        }
#elif MABAIN
        auto start = std::chrono::high_resolution_clock::now();
        db->Add(key, val);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
#endif

        if ((i + 1) % ONE_MILLION == 0) {
            std::cout << "inserted: " << (i + 1) << " key-value pairs\n";
        }
    }
#if LMDB
    if (!sync_on_write)
        mdb_txn_commit(txn);
#endif

    uint64_t timediff = total_time;
    std::cout << "===== " << timediff * 1.0 / n << " micro seconds per insertion\n";
}

static void Lookup(int n)
{
    int nfound = 0;
    char kv[65];
#ifdef LMDB
    MDB_val lmdb_key, lmdb_value;
    MDB_cursor* cursor;
    mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    mdb_cursor_open(txn, db, &cursor);
#endif

    uint64_t total_time = 0;
#ifdef MABAIN
    uint64_t total_time_ns = 0; // measure whole-loop time in nanoseconds
    mabain::MBData mbd; // reuse per-iteration buffer to avoid reallocation
    mbd.options = mabain::CONSTS::OPTION_KEY_ONLY;
    const char* prof_env = std::getenv("MB_PROFILE_FIND");
    if (prof_env && *prof_env && std::strcmp(prof_env, "0") != 0) {
        db->ResetFindProfileStats();
    }
#endif

#ifdef MABAIN
    // Dump stats before lookup; cache has been seeded by writer already
    std::cout << "-- Cache stats before lookup --\n";
    db->DumpPrefixCacheStats(std::cout);
    if (pc_cap > 0)
        db->ResetPrefixCacheStats();
#endif
    // Time each find call only (exclude key-building and loop overhead)
    for (int i = 0; i < n; i++) {
        std::string key;
        if (key_type == 0) {
            key = std::to_string(i);
        } else if (key_type == 1 || key_type == 2) {
            if (key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        } else { // u32 pseudo-random: 3LE bytes + decimal suffix; match Add()
            uint32_t r = prand_u64(static_cast<uint64_t>(i));
            key.assign((char*)&r, 4);
            key += std::to_string(i);
        }

#ifdef LEVEL_DB
        std::string value;
        auto start = std::chrono::high_resolution_clock::now();
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
        if (s.ok())
            nfound++;
#elif KYOTO_CABINET
        std::string value;
        auto start = std::chrono::high_resolution_clock::now();
        bool found = db->get(key.data(), key.size(), &value);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
        if (found)
            nfound++;
#elif LMDB
        lmdb_key.mv_data = (void*)key.data();
        lmdb_key.mv_size = key.size();
        auto start = std::chrono::high_resolution_clock::now();
        int lmdb_result = mdb_cursor_get(cursor, &lmdb_key, &lmdb_value, MDB_SET);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
        if (lmdb_result == 0)
            nfound++;
            // std::cout<<key<<":"<<std::string((char*)lmdb_value.mv_data, lmdb_value.mv_size)<<"\n";
#elif MABAIN
        struct timespec ts1, ts2;
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);
        int rval = db->Find(key.data(), (int)key.size(), mbd);
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts2);
        uint64_t delta_ns = (uint64_t)(ts2.tv_sec - ts1.tv_sec) * 1000000000ULL + (uint64_t)(ts2.tv_nsec - ts1.tv_nsec);
        total_time_ns += delta_ns;
        if (rval == 0)
            nfound++;
#endif

        if ((i + 1) % ONE_MILLION == 0) {
            std::cout << "looked up: " << (i + 1) << " keys\n";
        }
    }

#ifdef LMDB
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
#endif
    uint64_t timediff = total_time;
#ifdef MABAIN
    timediff = total_time_ns / 1000ULL; // convert ns to us at the end
#endif

    std::cout << "found " << nfound << " key-value pairs\n";
    std::cout << "===== " << timediff * 1.0 / n << " micro seconds per lookup\n";
// (unordered_map) test removed per request
#ifdef MABAIN
    if (db) {
        std::cout << "-- Cache stats after lookup --\n";
        db->DumpPrefixCacheStats(std::cout);
        const char* prof2 = std::getenv("MB_PROFILE_FIND");
        if (prof2 && *prof2 && std::strcmp(prof2, "0") != 0) {
            db->DumpFindProfileStats(std::cout);
        }
    }
#endif
}

static void Delete(int n)
{
    int nfound = 0;
    char kv[65];

#if LMDB
    if (!sync_on_write)
        mdb_txn_begin(env, NULL, 0, &txn);
#endif

    uint64_t total_time = 0;
#ifdef MABAIN
    uint64_t total_time_ns = 0; // accumulate in nanoseconds for precision
#endif
    for (int i = 0; i < n; i++) {
        std::string key;
        if (key_type == 0) {
            key = std::to_string(i);
        } else if (key_type == 1 || key_type == 2) {
            if (key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        } else { // u32 pseudo-random (binary 4 bytes, little-endian)
            uint32_t r = prand_u64(static_cast<uint64_t>(i));
            key.assign((char*)&r, 4);
        }
#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        auto start = std::chrono::high_resolution_clock::now();
        leveldb::Status s = db->Delete(opts, key);
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
        if (s.ok())
            nfound++;
#elif KYOTO_CABINET
        auto start = std::chrono::high_resolution_clock::now();
        bool removed = db->remove(key.data(), key.size());
        auto stop = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
        if (removed)
            nfound++;
#elif LMDB
        MDB_val lmdb_key;
        lmdb_key.mv_size = key.size();
        lmdb_key.mv_data = (void*)key.data();
        if (sync_on_write) {
            mdb_txn_begin(env, NULL, 0, &txn);
            auto start = std::chrono::high_resolution_clock::now();
            int result = mdb_del(txn, db, &lmdb_key, NULL);
            auto stop = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
            if (result == 0)
                nfound++;
            mdb_txn_commit(txn);
        } else {
            auto start = std::chrono::high_resolution_clock::now();
            int result = mdb_del(txn, db, &lmdb_key, NULL);
            auto stop = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
            if (result == 0)
                nfound++;
        }
#elif MABAIN
        struct timespec ts1, ts2;
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts1);
        int rval = db->Remove(key.data(), static_cast<int>(key.size()));
        clock_gettime(CLOCK_MONOTONIC_RAW, &ts2);
        uint64_t delta_ns = (uint64_t)(ts2.tv_sec - ts1.tv_sec) * 1000000000ULL + (uint64_t)(ts2.tv_nsec - ts1.tv_nsec);
        total_time_ns += delta_ns;
        if (rval == 0)
            nfound++;
#endif

        if ((i + 1) % ONE_MILLION == 0) {
            std::cout << "deleted: " << (i + 1) << " keys\n";
        }
    }

#if LMDB
    if (!sync_on_write)
        mdb_txn_commit(txn);
#endif

    std::cout << "deleted " << nfound << " key-value pairs\n";
    uint64_t timediff = total_time;
#ifdef MABAIN
    timediff = total_time_ns / 1000ULL; // convert ns to us
#endif
    std::cout << "===== " << timediff * 1.0 / n << " micro seconds per deletion\n";
}

static void* Writer(void* arg)
{
    int num = *((int*)arg);
    char kv[65];
    int tid = static_cast<int>(syscall(SYS_gettid));

    std::cout << "\n[writer : " << tid << "] started" << std::endl;
    for (int i = 0; i < num; i++) {
        std::string key, val;
        if (key_type == 0) {
            key = std::to_string(i);
            val = std::to_string(i);
        } else if (key_type == 1 || key_type == 2) {
            if (key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
            val = kv;
        } else { // u32 pseudo-random (binary 4 bytes, little-endian)
            uint32_t r = prand_u64(static_cast<uint64_t>(i));
            key.assign((char*)&r, 4);
            val = key;
        }

#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        db->Put(opts, key, val);
#elif MABAIN
        db->Add(key.c_str(), key.length(), val.c_str(), val.length());
#ifdef DEFRAG
        if (!use_jemalloc) {
            if ((i + 1) % (2 * ONE_MILLION) == 0) {
                std::cout << "\nRC SCHEDULED " << std::endl;
                db->CollectResource(1, 1);
            }
        }
#endif
#endif

        if ((i + 1) % ONE_MILLION == 0) {
            std::cout << "\nwriter inserted " << (i + 1) << std::endl;
        }
    }

#ifdef MABAIN
    db->PrintStats();
#endif
    return NULL;
}

static void* Reader(void* arg)
{
    int num = *((int*)arg);
    int i = 0;
    int tid = static_cast<int>(syscall(SYS_gettid));
    char kv[65];

#if MABAIN
    std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
    mabain::DB* db_r = new mabain::DB(db_dir_tmp.c_str(), mabain::CONSTS::ReaderOptions(),
        (unsigned long long)(0.6666667 * memcap),
        (unsigned long long)(0.3333333 * memcap));
    assert(db_r->is_open());
    // Configure shared prefix cache for reader when -pcc is provided
    if (pc_cap > 0) {
        db_r->EnableSharedPrefixCache(static_cast<size_t>(pc_cap));
        // Read path does not write to cache; no need to toggle read-only
    }
#endif

    std::cout << "\n[reader : " << tid << "] started" << std::endl;
    while (i < num) {
        std::string key;
        bool found = false;

        if (key_type == 0) {
            key = std::to_string(i);
        } else if (key_type == 1 || key_type == 2) {
            if (key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        } else { // u32 pseudo-random (binary 4 bytes, little-endian)
            uint32_t r = prand_u64(static_cast<uint64_t>(i));
            key.assign((char*)&r, 4);
        }

        std::string value;
#ifdef LEVEL_DB
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        if (s.ok()) {
            found = true;
        }
#elif MABAIN
        mabain::MBData mbd;
        // Avoid value reads to reduce contention during concurrency
        mbd.options = mabain::CONSTS::OPTION_KEY_ONLY;
        int rval = db_r->Find(key, mbd);
        if (rval == 0) {
            found = true;
        }
#endif
        // Always advance to avoid indefinite spin if writer lags on a key
        if ((++i) % ONE_MILLION == 0) {
            std::cout << "\n[reader : " << tid << "] progressed to " << i << "\n";
        }
    }

#if MABAIN
    // Buffer per-reader cache stats before closing to avoid interleaving
    {
        std::ostringstream oss;
        oss << "-- Cache stats for reader tid " << tid << " --\n";
        db_r->DumpPrefixCacheStats(oss);
        pthread_mutex_lock(&g_stats_mutex);
        g_reader_stats.push_back(oss.str());
        pthread_mutex_unlock(&g_stats_mutex);
    }
    db_r->Close();
    delete db_r;
#endif
    return NULL;
}

static void ConcurrencyTest(int num, int n_r)
{
#ifdef KYOTO_CABINET
    std::cout << "===== concurrency test ignored for kyotocabinet\n";
    return;
#elif LMDB
    std::cout << "===== concurrency test ignored for lmdb\n";
    return;
#endif

    pthread_t wid;

    auto start = std::chrono::high_resolution_clock::now();

    // Clear buffered reader stats
    pthread_mutex_lock(&g_stats_mutex);
    g_reader_stats.clear();
    pthread_mutex_unlock(&g_stats_mutex);

    // No cache writes from readers during concurrency to avoid stalls
    // For shared cache, pre-create mapping to avoid races between threads/processes
#ifdef MABAIN
    if (pc_cap > 0) {
        std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
        mabain::DB* seed = new mabain::DB(db_dir_tmp.c_str(), mabain::CONSTS::ReaderOptions(),
            (unsigned long long)(0.6666667 * memcap),
            (unsigned long long)(0.3333333 * memcap));
        if (seed && seed->is_open()) {
            seed->EnableSharedPrefixCache(static_cast<size_t>(pc_cap));
            seed->Close();
        }
        delete seed;
    }
#endif

    // Start the writer
    if (pthread_create(&wid, NULL, Writer, &num) != 0) {
        std::cerr << "failed to create writer thread\n";
        abort();
    }

    // start the readers
    pthread_t rid[256];
    assert(n_r <= 256);
    for (int i = 0; i < n_r; i++) {
        if (pthread_create(&rid[i], NULL, Reader, &num) != 0) {
            std::cerr << "failed to create writer thread\n";
            abort();
        }
    }

    for (int i = 0; i < n_r; i++) {
        pthread_join(rid[i], NULL);
    }
    pthread_join(wid, NULL);

    auto stop = std::chrono::high_resolution_clock::now();

    uint64_t timediff = std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count();
    std::cout << "===== " << timediff * 1.0 / num_kv << " micro seconds per concurrent insertion/lookup\n";
#ifdef MABAIN
    if (db) {
        std::cout << "-- Cache stats after concurrency --\n";
        db->DumpPrefixCacheStats(std::cout);
    }
    // Print buffered per-reader cache stats
    pthread_mutex_lock(&g_stats_mutex);
    for (const auto& s : g_reader_stats) {
        std::cout << s;
    }
    pthread_mutex_unlock(&g_stats_mutex);
#endif
}

static void DestroyDB()
{
#ifdef LEVEL_DB
    delete db;
    db = NULL;
#elif KYOTO_CABINET
    db->close();
    delete db;
    db = NULL;
#elif LMDB
    mdb_close(env, db);
    mdb_env_close(env);
#elif MABAIN
    db->Close();
    delete db;
    db = NULL;
#endif
}

static void RemoveDB()
{
    std::string cmd = std::string("rm -rf ") + db_dir;
#ifdef LEVEL_DB
    cmd += "leveldb/*";
#elif KYOTO_CABINET
    cmd += "kyotocabinet/*";
#elif LMDB
    cmd += "lmdb/*";
#elif MABAIN
    cmd += "mabain/*";
    mabain::DB::ClearResources(std::string("/var/tmp/"));
#endif

    if (system(cmd.c_str()) != 0) {
    }
}

int main(int argc, char* argv[])
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-n") == 0) {
            if (++i >= argc)
                abort();
            num_kv = atoi(argv[i]);
        } else if (strcmp(argv[i], "-k") == 0) {
            if (++i >= argc)
                abort();
            if (strcmp(argv[i], "int") == 0) {
                key_type = 0;
            } else if (strcmp(argv[i], "sha1") == 0) {
                key_type = 1;
            } else if (strcmp(argv[i], "sha2") == 0) {
                key_type = 2;
            } else if (strcmp(argv[i], "u32") == 0) {
                key_type = 3; // deterministic pseudo-random uint32
            } else {
                std::cerr << "invalid key type: " << argv[i] << "\n";
                abort();
            }
        } else if (strcmp(argv[i], "-t") == 0) {
            if (++i >= argc)
                abort();
            n_reader = atoi(argv[i]);
        } else if (strcmp(argv[i], "-d") == 0) {
            if (++i >= argc)
                abort();
            db_dir = argv[i];
        } else if (strcmp(argv[i], "-s") == 0) {
            sync_on_write = true;
        } else if (strcmp(argv[i], "-m") == 0) {
            if (++i >= argc)
                abort();
            memcap = atoi(argv[i]);
        } else if (strcmp(argv[i], "-j") == 0) {
            use_jemalloc = true;
        } else if (strcmp(argv[i], "-pcc") == 0) {
            if (++i >= argc)
                abort();
            pc_cap = atoll(argv[i]);
        } else {
            std::cerr << "invalid argument: " << argv[i] << "\n";
        }
    }

#ifdef MABAIN
    // Set log file after parsing -d so it lands under the chosen dir
    // Default to current workspace to avoid restricted paths
    std::string log_path = std::string(db_dir) + std::string("/mabain.log");
    mabain::DB::SetLogFile(log_path.c_str());
#endif
    print_cpu_info();
    if (sync_on_write)
        std::cout << "===== Disk sync is on\n";
    else
        std::cout << "===== Disk sync is off\n";
    std::cout << "===== Memcap is " << memcap << "\n";
#ifdef MABAIN
    if (pc_cap > 0) {
        std::cout << "===== Prefix cache cap=" << pc_cap << "\n";
    }
#endif

    InitTestDir();
    RemoveDB();

    InitDB();
    Add(num_kv);
    DestroyDB();

    InitDB(false);
    Lookup(num_kv);
    DestroyDB();

    InitDB(); // optional delete phase disabled
    Delete(num_kv);
    DestroyDB();

    RemoveDB();

    InitDB();
    ConcurrencyTest(num_kv, n_reader);
    DestroyDB();

#ifdef MABAIN
    mabain::DB::CloseLogFile();
#endif
    return 0;
}
