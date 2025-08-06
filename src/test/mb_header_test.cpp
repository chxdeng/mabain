#include <assert.h>
#include <fstream>
#include <time.h>
#include <unistd.h>
#include <filesystem>

#include "../db.h"

using namespace std;
using namespace mabain;

const char* db_dir = "/var/tmp/mabain_test";

static void create_header_file(int size)
{
    char hdr[size];
    for (int i = 0; i < size; i++) {
        hdr[i] = (char)(rand() % 256);
    }
    string path = string(db_dir) + "/_mabain_h";
    ofstream wf(path.c_str(), ios::out | ios::binary);
    wf.write(hdr, size);
    wf.close();
}

int main(int argc, char* argv[])
{
    srand(time(NULL));
    
    try {
        // Remove files matching pattern
        for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
            if (entry.path().filename().string().find("_mabain") == 0) {
                std::filesystem::remove_all(entry.path());
            }
        }
    } catch (const std::filesystem::filesystem_error& ex) {
        // Ignore errors, similar to original system() call
    }
    
    create_header_file(4096);
    DB db(db_dir, CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE);
    assert(db.is_open());
    usleep(1000000);
    return 0;
}
