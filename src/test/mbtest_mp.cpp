#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "../db.h"
#include "./test_key.h"

const char *db_dir = "/var/tmp/mabain_test";

using namespace mabain;

int main(int argc, char *argv[])
{
    int num = 0;
    int options = CONSTS::ReaderOptions();
    int duration = 0; // in seconds
    int n0 = 0;
    int key_type = MABAIN_TEST_KEY_TYPE_INT;
    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i], "-w") == 0) {
            options |= CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
        } else if(strcmp(argv[i], "-d") == 0) {
            if(++i >= argc) abort();
            db_dir = argv[i];
        } else if(strcmp(argv[i], "-n") == 0) {
            if(++i >= argc) abort();
            num = atoi(argv[i]);
        } else if(strcmp(argv[i], "-n0") == 0) {
            if(++i >= argc) abort();
            n0 = atoi(argv[i]);
        } else if(strcmp(argv[i], "-t") == 0) {
            if(++i >= argc) abort();
            duration = atoi(argv[i]);
        } else if(strcmp(argv[i], "-k") == 0) {
            if(++i >= argc) abort();
	    if(strcmp(argv[i], "int") == 0) {
                key_type = MABAIN_TEST_KEY_TYPE_INT;
            } else if(strcmp(argv[i], "sha1") == 0) {
                key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
            } else {
                key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
            }
        } else {
            std::cout << "unknown argument " << argv[i] << "\n";
        }
    }

    DB::SetLogFile(std::string(db_dir) + "/mabain.log");
    DB *db = new DB(db_dir, options);
    TestKey tkey(key_type);
    assert(db->is_open());

    for(int i = 0; i < num; i++) {
        std::string kv = tkey.get_key(n0 + i);
        int rval;
        do {
            rval = db->Add(kv, kv);
        } while(rval == MBError::TRY_AGAIN);
        if(rval != MBError::SUCCESS) {
            std::cout << "failed to add " << kv << " :" << MBError::get_error_str(rval) << std::endl;
        }
    }

    if(duration > 0) {
        uint32_t tm_stop = time(NULL) + duration;
        int tm_diff = tm_stop - time(NULL);
        while(tm_diff > 0) {
            sleep(1);
            tm_diff = tm_stop - time(NULL); 
        }
        std::cout << "async writer exited\n";
    }

    db->Close();
    DB::CloseLogFile();
    return 0;
}
