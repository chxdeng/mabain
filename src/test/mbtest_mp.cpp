#include <assert.h>
#include <unistd.h>

#include <../db.h>

const char *db_dir = "/var/tmp/mabain_test";

using namespace mabain;

int main(int argc, char *argv[])
{
    int num = 0;
    int options = CONSTS::ReaderOptions();
    int duration = 0; // in seconds
    int n0 = 0;
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
        } else {
            std::cout << "unknown argument " << argv[i] << "\n";
        }
    }

    uint32_t tm_stop = time(NULL) + duration;

    DB *db = new DB(db_dir, options);
    assert(db->is_open());

    for(int i = 0; i < num; i++) {
        std::string kv = std::to_string(n0 + i);
        db->Add(kv, kv);
    }

    while(time(NULL) < tm_stop) {

        sleep(1);
    }

    db->Close();
    return 0;
}
