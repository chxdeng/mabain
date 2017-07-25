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
#else
#include <mabain/db.h>
#endif

#ifdef LEVEL_DB
leveldb::DB *db = NULL;
#elif KYOTO_CABINET
kyotocabinet::PolyDB *db = NULL;
#else
mabain::DB *db = NULL;
#endif

static const char *db_dir = "/var/tmp/db_test/";
static int num_kv = 1000000;
static int n_reader = 7;
static int key_type = 0;

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

static void InitDB()
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
#else
    std::cout << "===== using mabain for testing\n";
    std::string db_dir_tmp = std::string(db_dir) + "/mabain/";
#endif

    cmd = std::string("mkdir -p ") + db_dir_tmp;
    if(system(cmd.c_str()) != 0) {
    }
    cmd = std::string("rm -rf ") + db_dir_tmp + "*";
    if(system(cmd.c_str()) != 0) {
    }

#ifdef LEVEL_DB
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_dir_tmp, &db);
    assert(status.ok());
#elif KYOTO_CABINET
    db = new kyotocabinet::PolyDB();
    std::string db_path = db_dir_tmp + "casket.kch";
    if (!db->open(db_path.c_str(), kyotocabinet::PolyDB::OWRITER | kyotocabinet::PolyDB::OCREATE)) {
        std::cerr << "failed to open kyotocabinet db\n";
        abort();
    }
#else
    db = new mabain::DB(db_dir_tmp, 64*1024*1024LL, 0*1024*1024LL, mabain::CONSTS::ACCESS_MODE_WRITER);
    assert(db->is_open());
#endif
}

static void Add(int n)
{
    timeval start, stop;
    char kv[65];

    gettimeofday(&start,NULL);
    for(int i = 0; i < n; i++) {
        std::string key, val;
        if(key_type == 0) {
            key = "db-comparison-key" + std::to_string(i);
            val = "db-comparison-value" + std::to_string(i);
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
        db->Put(leveldb::WriteOptions(), key, val);
#elif KYOTO_CABINET
        db->set(key.c_str(), val.c_str());
#else
        db->Add(key.c_str(), key.length(), val.c_str(), val.length());
#endif

        if((i+1)%1000000 == 0) {
            std::cout << "inserted: " << (i+1) << " key-value pairs\n";
        }
    }    
    gettimeofday(&stop,NULL);

    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/n << " micro seconds per insertion\n";
}

static void Lookup(int n)
{
    timeval start, stop;
    int nfound = 0;
    char kv[65];

    gettimeofday(&start,NULL);
    for(int i = 0; i < n; i++) {
        std::string key;
        if(key_type == 0) {
            key = "db-comparison-key" + std::to_string(i);
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
#else
        mabain::MBData mbd;
        int rval = db->Find(key.c_str(), key.length(), mbd);
        if(rval == 0) nfound++;
#endif

        if((i+1)%1000000 == 0) {
            std::cout << "looked up: " << (i+1) << " keys\n";
        }
    }
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

    gettimeofday(&start,NULL);
    for(int i = 0; i < n; i++) {
        std::string key;
        if(key_type == 0) {
            key = "db-comparison-key" + std::to_string(i);
        } else {
            if(key_type == 1) {
                get_sha1_str(i, kv);
            } else {
                get_sha256_str(i, kv);
            }
            key = kv;
        }
#ifdef LEVEL_DB
        leveldb::Status s = db->Delete(leveldb::WriteOptions(), key);
        if(s.ok()) nfound++;
#elif KYOTO_CABINET
        std::string value;
        if(db->remove(key)) nfound++;
#else
        int rval = db->Remove(key.c_str(), key.length());
        if(rval == 0) nfound++;
#endif

        if((i+1)%1000000 == 0) {
            std::cout << "deleted: " << (i+1) << " keys\n";
        }
    }
    gettimeofday(&stop,NULL);

    std::cout << "deleted " << nfound << " key-value pairs\n";
    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/n << " micro seconds per deletion\n";
}

static void *Writer(void *arg)
{
    int num = *((int *) arg);
    char kv[65];

    std::cout << "writer started " << "\n";
    for(int i = 0; i < num; i++) {
        std::string key, val;
        if(key_type == 0) {
            key = "db-comparison-key" + std::to_string(i);
            val = "db-comparison-value" + std::to_string(i);
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
        db->Put(leveldb::WriteOptions(), key, val);
#elif KYOTO_CABINET
#else
        db->Add(key.c_str(), key.length(), val.c_str(), val.length());
#endif

        if((i+1)%1000000 == 0) {
            std::cout << "writer inserted " << (i+1) << "\n";
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

    std::cout << "reader " << tid << " started " << "\n";
    while(i < num) {
        std::string key;
        if(key_type == 0) {
            key = "db-comparison-key" + std::to_string(i);
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
            std::string v;
            if(key_type == 0) {
                v = "db-comparison-value" + std::to_string(i);
            } else {
                v = key;
            }
            if(value.compare(v) != 0) {
                std::cout << "VALUE NOT MATCH for key:" << key << ":" << value << "\n";
                abort();
            }
            i++;
        }
#elif KYOTO_CABINET
#else
        mabain::MBData mbd;
        int rval = db->Find(key.c_str(), key.length(), mbd);
        if(rval == 0) {
            std::string v;
            if(key_type == 0) {
                v = "db-comparison-value" + std::to_string(i);
            } else {
                v = key;
            }
            value = std::string((const char *)mbd.buff, mbd.data_len);
            if(value.compare(v) != 0) {
                std::cout << "VALUE NOT MATCH for key:" << key << ":" << value << "\n";
                abort();
            }
            i++;
        }
#endif

        if((i+1)%1000000 == 0) {
            std::cout << "reader " << tid << " found " << (i+1) << "\n";
        }
    }

    return NULL;
}
static void ConcurrencyTest(int num, int n_r)
{
#ifdef KYOTO_CABINET
    std::cout << "===== concurrency test ignored for kyotocabinet\n";
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

    pthread_join(wid, NULL);
    for(int i = 0; i < n_r; i++) {
        pthread_join(rid[i], NULL);
    }

    gettimeofday(&stop,NULL);

    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "===== " << timediff*1.0/num_kv << " micro seconds per concurrent insertion/lookup\n";
}

static void DestroyDB()
{
#ifdef LEVEL_DB
#elif KYOTO_CABINET
    db->close();
#else
    db->Close();
#endif

    delete db;
    db = NULL;
}

int main(int argc, char *argv[])
{
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
        } else {
            std::cerr << "invalid argument: " << argv[i] << "\n";
        }
    }

    print_cpu_info();

    InitDB();
    Add(num_kv);
    Lookup(num_kv);
    Delete(num_kv);
    DestroyDB();

    InitDB();
    ConcurrencyTest(num_kv, n_reader);
    DestroyDB();

    return 0;
}
