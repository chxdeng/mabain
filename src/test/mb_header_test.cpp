#include <fstream>
#include <time.h>
#include <assert.h> 
#include <unistd.h> 

#include "../db.h"

using namespace std;
using namespace mabain;

const char *db_dir = "/var/tmp/mabain_test";

static void create_header_file(int size) {
    char hdr[size];
    for (int i = 0; i < size; i++) {
        hdr[i] = (char) (rand() % 256);
    }
    string path = string(db_dir) + "/_mabain_h";
    ofstream wf(path.c_str(), ios::out | ios::binary);
    wf.write(hdr, size);
    wf.close();
}

int main(int argc, char *argv[])
{
    srand(time(NULL));
    string cmd = string("rm ") + db_dir + "/_mabain*";
    if (system(cmd.c_str()) != 0) {
    }
    create_header_file(4096);
    DB db(db_dir, CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE);
    assert(db.is_open());
    usleep(1000000);
    return 0;
}
