#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <sys/time.h>
#include <assert.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <openssl/sha.h>

#ifdef LEVEL_DB
#include <leveldb/db.h>
#elif KYOTO_CABINET
#include <kcpolydb.h>
#include <kchashdb.h>
#elif LMDB
extern "C" {
#include <lmdb.h>
}
#elif MABAIN
#include <mabain/db.h>
#endif

#ifdef LEVEL_DB
leveldb::DB *db = NULL;
#elif KYOTO_CABINET
kyotocabinet::HashDB *db = NULL;
#elif LMDB
MDB_env *env = NULL;
MDB_dbi db;
MDB_txn *txn = NULL;
#elif MABAIN
mabain::DB *db = NULL;
#endif

#define ONE_MILLION 1000000

static const char *db_dir   = "/var/tmp/db_test/";
static int num_kv           = 1 * ONE_MILLION;
static int n_reader         = 7;
static int key_type         = 0;
static bool sync_on_write   = false;
static unsigned long long memcap = 1024ULL*1024*1024;

static void get_sha256_str(int key, char *sha256_str)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, (unsigned char*)&key, 4);
    SHA256_Final(hash, &sha256);
    int i = 0;
    for(i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        sprintf(sha256_str + (i * 2), "%02x", hash[i]);
    }
    sha256_str[64] = 0;
}

static void get_sha1_str(int key, char *sha1_str)
{
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    SHA1_Update(&sha1, (unsigned char*)&key, 4);
    SHA1_Final(hash, &sha1);
    int i = 0;
    for(i = 0; i < SHA_DIGEST_LENGTH; i++)
    {
        sprintf(sha1_str + (i * 2), "%02x", hash[i]);
    }
    sha1_str[32] = 0;
}

static void print_cpu_info()
{
    std::ifstream cpu_info("/proc/cpuinfo", std::fstream::in);
    if(!cpu_info.is_open()) {
        return;
    }

    std::string line;
    std::string model_name;
    std::string vendor_id;
    std::string cpu_freq;
    int n_processor = 0;
    while (std::getline(cpu_info, line))
    {
        if(line.find("model name") != std::string::npos)
        {
            model_name = line;
            n_processor++;
        }
        else if(line.find("vendor_id") != std::string::npos)
        {
            vendor_id = line;
        }
        else if(line.find("cpu MHz") != std::string::npos)
        {
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
    if(system(cmd.c_str()) != 0) {
    }

#ifdef LEVEL_DB
    std::cout << "===== using leveldb for testing\n";
    std::string db_dir_tmp = std::string(db_dir) + "/leveldb/";
#elif KYOTO_CABINET
    std::cout << "===== using kyotocabinet for testing\n";
    std::string db_dir_tmp = std::string(db_dir) + "/kyotocabinet/";
#elif LMDB
    std::cout << "===== using lmdb for testing\n";
    std::string db_dir_tmp = std::string(db_dir) + "/lmdb/";
#elif MABAIN
    std::cout << "===== using mabain for testing\n";
    std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
#endif
    cmd = std::string("mkdir -p ") + db_dir_tmp;
    if(system(cmd.c_str()) != 0) {
    }
    cmd = std::string("rm -rf ") + db_dir_tmp + "*";
    if(system(cmd.c_str()) != 0) {
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
    int open_options = kyotocabinet::PolyDB::OWRITER |
                       kyotocabinet::PolyDB::OCREATE;
    db->tune_map(memcap);
    if(!db->open(db_file, open_options)) {
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
    std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
    int options = mabain::CONSTS::WriterOptions() | mabain::CONSTS::ASYNC_WRITER_MODE;
    if(sync_on_write) {
        options |= mabain::CONSTS::SYNC_ON_WRITE;
    }
    if(!writer_mode)
        options = mabain::CONSTS::ReaderOptions();
    db = new mabain::DB(db_dir_tmp.c_str(), options, (unsigned long long)(0.6666667*memcap),
                                                     (unsigned long long)(0.3333333*memcap));
    assert(db->is_open());
#endif
}

static void Add(int n)
{
    timeval start, stop;
    char kv[65];

    gettimeofday(&start,NULL);
#if LMDB
    if(!sync_on_write)
        mdb_txn_begin(env, NULL, 0, &txn);
#endif
    for(int i = 0; i < n; i++) {
        std::string key, val;
        if(key_type == 0) {
            key = std::to_string(i);
            val = std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
            val = kv;
        }

#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        db->Put(opts, key, val);
#elif KYOTO_CABINET
        db->set(key.c_str(), val.c_str());
#elif LMDB
        MDB_val lmdb_key, lmdb_val;
        lmdb_key.mv_size = key.size();
        lmdb_key.mv_data = (void*) key.data();
        lmdb_val.mv_size = val.size();
        lmdb_val.mv_data = (void*) val.data();
        MDB_cursor *mc;
        if(sync_on_write) {
            mdb_txn_begin(env, NULL, 0, &txn);
            mdb_cursor_open(txn, db, &mc);
            mdb_cursor_put(mc, &lmdb_key, &lmdb_val, 0);
            mdb_cursor_close(mc);
            mdb_txn_commit(txn);
        } else {
            mdb_cursor_open(txn, db, &mc);
            mdb_cursor_put(mc, &lmdb_key, &lmdb_val, 0);
            mdb_cursor_close(mc);
        }
#elif MABAIN
        db->Add(key, val);
#endif

        if((i+1) % ONE_MILLION == 0) {
            std::cout << "inserted: " << (i+1) << " key-value pairs\n";
        }
    }    
#if LMDB
    if(!sync_on_write)
        mdb_txn_commit(txn);
#endif

    gettimeofday(&stop,NULL);

    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/n << " micro seconds per insertion\n";
}

static void Lookup(int n)
{
    timeval start, stop;
    int nfound = 0;
    char kv[65];
#ifdef LMDB
    MDB_val lmdb_key, lmdb_value;
    MDB_cursor *cursor;
    mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    mdb_cursor_open(txn, db, &cursor);
#endif

    gettimeofday(&start,NULL);
    for(int i = 0; i < n; i++) {
        std::string key;
        if(key_type == 0) {
            key = std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        }

#ifdef LEVEL_DB
        std::string value;
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        if(s.ok()) nfound++;
#elif KYOTO_CABINET
        std::string value;
        if(db->get(key, &value)) nfound++;
#elif LMDB
        lmdb_key.mv_data = (void*) key.data();
        lmdb_key.mv_size = key.size();
        if(mdb_cursor_get(cursor, &lmdb_key, &lmdb_value, MDB_SET) == 0)
            nfound++;
        //std::cout<<key<<":"<<std::string((char*)lmdb_value.mv_data, lmdb_value.mv_size)<<"\n";
#elif MABAIN
        mabain::MBData mbd;
        int rval = db->Find(key, mbd);
        //std::cout<<key<<":"<<std::string((char*)mbd.buff, mbd.data_len)<<"\n";
        if(rval == 0) nfound++;
#endif

        if((i+1) % ONE_MILLION == 0) {
            std::cout << "looked up: " << (i+1) << " keys\n";
        }
    }

#ifdef LMDB
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
#endif
    gettimeofday(&stop,NULL);

    std::cout << "found " << nfound << " key-value pairs\n";
    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/n << " micro seconds per lookup\n";
}

static void Delete(int n)
{
    timeval start, stop;
    int nfound = 0;
    char kv[65];
#if LMDB
    if(!sync_on_write)
        mdb_txn_begin(env, NULL, 0, &txn);
#endif

    gettimeofday(&start,NULL);
    for(int i = 0; i < n; i++) {
        std::string key;
        if(key_type == 0) {
            key = std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        }
#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        leveldb::Status s = db->Delete(opts, key);
        if(s.ok()) nfound++;
#elif KYOTO_CABINET
        std::string value;
        if(db->remove(key)) nfound++;
#elif LMDB
        MDB_val lmdb_key;
        lmdb_key.mv_size = key.size();
        lmdb_key.mv_data = (void*) key.data();
        if(sync_on_write) {
            mdb_txn_begin(env, NULL, 0, &txn);
            if(mdb_del(txn, db, &lmdb_key, NULL) == 0) nfound++;
            mdb_txn_commit(txn);
        } else {
            if(mdb_del(txn, db, &lmdb_key, NULL) == 0) nfound++;
        }
#elif MABAIN
        int rval = db->Remove(key);
        if(rval == 0) nfound++;
#endif

        if((i+1) % ONE_MILLION == 0) {
            std::cout << "deleted: " << (i+1) << " keys\n";
        }
    }

#if LMDB
    if(!sync_on_write)
        mdb_txn_commit(txn);
#endif
    gettimeofday(&stop,NULL);

    std::cout << "deleted " << nfound << " key-value pairs\n";
    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/n << " micro seconds per deletion\n";
}

static void *Writer(void *arg)
{
    int num = *((int *) arg);
    char kv[65];

    std::cout << "\nwriter started " << std::endl;
    for(int i = 0; i < num; i++) {
        std::string key, val;
        if(key_type == 0) {
            key = std::to_string(i);
            val = std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
            val = kv;
        }

#ifdef LEVEL_DB
        leveldb::WriteOptions opts = leveldb::WriteOptions();
        opts.sync = sync_on_write;
        db->Put(opts, key, val);
#elif MABAIN
        db->Add(key.c_str(), key.length(), val.c_str(), val.length());
#ifdef DEFRAG
        if((i+1) % (2*ONE_MILLION)== 0) {
            std::cout<<"\nRC SCHEDULED " << std::endl;
            // db->CollectResource(2*ONE_MILLION, 64*ONE_MILLION);
            db->CollectResource(1, 1);
        }
#endif
#endif

        if((i+1) % ONE_MILLION == 0) {
            std::cout << "\nwriter inserted " << (i+1) << std::endl;
        }
    }

    return NULL;
}
static void *Reader(void *arg)
{
    int num = *((int *) arg);
    int i = 0;
    int tid = static_cast<int>(syscall(SYS_gettid));
    char kv[65];

#if MABAIN
    std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
    mabain::DB *db_r = new mabain::DB(db_dir_tmp.c_str(), mabain::CONSTS::ReaderOptions(),
                                      (unsigned long long)(0.6666667*memcap),
                                      (unsigned long long)(0.3333333*memcap));
    assert(db_r->is_open());
#endif

    std::cout << "\n[reader : " << tid << "] started" << std::endl;
    while(i < num) {
        std::string key;
        bool found = false;

        if(key_type == 0) {
            key = std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        }

        std::string value;
#ifdef LEVEL_DB
        leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);
        if(s.ok()) {
            found = true;
        }
#elif MABAIN
        mabain::MBData mbd;
        int rval = db_r->Find(key, mbd);
        if(rval == 0) {
            value = std::string((const char *)mbd.buff, mbd.data_len);
            found = true;
        }
#endif
        if(found) {
            if(key.compare(value) != 0) {
                std::cout << "\nVALUE NOT MATCH for key:" << key << ":" << value << "\n";
                abort();
            }

            i++;
            if((i+1) % ONE_MILLION == 0) {
                std::cout << "\n[reader : " << tid << "] found " << (i+1) << "\n";
            }
        }
    }

#if MABAIN
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
    timeval start, stop;

    gettimeofday(&start,NULL);

    // Start the writer
    if(pthread_create(&wid, NULL, Writer, &num) != 0) {
        std::cerr << "failed to create writer thread\n";
        abort();
    }    

    // start the readers
    pthread_t rid[256];
    assert(n_r <= 256);
    for(int i = 0; i < n_r; i++) {
        if(pthread_create(&rid[i], NULL, Reader, &num) != 0) {
            std::cerr << "failed to create writer thread\n";
            abort();
        }    
    }

    for(int i = 0; i < n_r; i++) {
        pthread_join(rid[i], NULL);
    }
    pthread_join(wid, NULL);

    gettimeofday(&stop,NULL);

    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/num_kv << " micro seconds per concurrent insertion/lookup\n";
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

    if(system(cmd.c_str()) != 0) {
    }
}

int main(int argc, char *argv[])
{
#ifdef MABAIN
    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    // mabain::DB::SetLogLevel(2);
#endif
    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i], "-n") == 0) {
            if(++i >= argc) abort();
            num_kv = atoi(argv[i]);
        } else if(strcmp(argv[i], "-k") == 0) {
            if(++i >= argc) abort();
            if(strcmp(argv[i], "int") == 0) {
                key_type = 0;
            } else if(strcmp(argv[i], "sha1") == 0) {
                key_type = 1;
            } else if(strcmp(argv[i], "sha2") == 0) {
                key_type = 2;
            } else {
                std::cerr << "invalid key type: " << argv[i] << "\n";
                abort();
            }
        } else if(strcmp(argv[i], "-t") == 0) {
            if(++i >= argc) abort();
            n_reader = atoi(argv[i]);
        } else if(strcmp(argv[i], "-d") == 0) {
            if(++i >= argc) abort();
            db_dir = argv[i];
        } else if(strcmp(argv[i], "-s") == 0) {
            sync_on_write = true;
        } else if(strcmp(argv[i], "-m") == 0) {
            if(++i >= argc) abort();
            memcap = atoi(argv[i]);
        } else {
            std::cerr << "invalid argument: " << argv[i] << "\n";
        }
    }

    print_cpu_info();
    if(sync_on_write)
        std::cout << "===== Disk sync is on\n";
    else
        std::cout << "===== Disk sync is off\n";
    std::cout << "===== Memcap is " << memcap << "\n";

    InitTestDir();
    RemoveDB();

    InitDB();
    Add(num_kv);
    DestroyDB();

    InitDB(false);
    Lookup(num_kv);
    DestroyDB();

    InitDB();
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
