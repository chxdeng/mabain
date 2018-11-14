/**
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

#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <atomic>

#include <mabain/db.h>

#include "test_key.h"

using namespace mabain;

static int max_key = 1000000;
static std::atomic<int> write_index;
static bool stop_processing = false;
static std::string mbdir = "/var/tmp/mabain_test/";

static void* insert_thread(void *arg)
{
    int curr_key;
    TestKey mkey(MABAIN_TEST_KEY_TYPE_INT);
    std::string kv;
    DB *db_r = new DB(mbdir.c_str(), CONSTS::ReaderOptions(), 128LL*1024*1024, 128LL*1024*1024);
    assert(db_r->is_open());

    while(!stop_processing) {
        curr_key = write_index.fetch_add(1, std::memory_order_release);
        kv = mkey.get_key(curr_key);
        if(curr_key < max_key) {
            assert(db_r->Add(kv, kv) == MBError::SUCCESS);
        } else {
            stop_processing = true;
            break;
        }
    }

    // Reader must unregister the async writer pointer
    delete db_r;
    return NULL;
}

static void SetTestStatus(bool success)
{
    std::string cmd;
    if(success) {
        cmd = std::string("touch ") + mbdir + "/_success";
    } else {
        cmd = std::string("rm ") + mbdir + "/_success >" + mbdir + "/out 2>" + mbdir + "/err";
    }
    if(system(cmd.c_str()) != 0) {
    }
}

static void Lookup()
{
    TestKey mkey(MABAIN_TEST_KEY_TYPE_INT);
    std::string kv;
    DB *db_r = new DB(mbdir.c_str(), CONSTS::ReaderOptions(), 128LL*1024*1024, 128LL*1024*1024);
    assert(db_r->is_open());
    MBData mbd;

    for(int i = 0; i < max_key; i++) {
        kv = mkey.get_key(i);
        assert(db_r->Find(kv, mbd) == MBError::SUCCESS);
        assert(kv == std::string((const char *)mbd.buff, mbd.data_len));
    }
    db_r->Close();
    delete db_r;
}

// Multiple threads performing DB insertion/deletion/updating
int main(int argc, char *argv[])
{
    pthread_t pid[256];
    int nthread = 4;
    if(nthread > 256) {
        abort();
    }

    if(argc > 1) {
        mbdir = std::string(argv[1]);
        std::cout << "Mabain test db directory " << mbdir << "\n";
    }
    if(argc > 2) {
        max_key = atoi(argv[2]);
        std::cout << "Setting number of keys to be " << max_key << "\n";
    }

    SetTestStatus(false);
    mabain::DB::SetLogFile(mbdir + "/mabain.log");

    write_index.store(0, std::memory_order_release);
    // Writer needs to enable async writer mode.
    int options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
    DB *db = new DB(mbdir.c_str(), options, 128LL*1024*1024, 128LL*1024*1024);
    assert(db->is_open());
    db->RemoveAll();

    for(int i = 0; i < nthread; i++) {
        if(pthread_create(&pid[i], NULL, insert_thread, db) != 0) {
            std::cout << "failed to create thread\n";
            abort();
        }
    }

    while(!stop_processing) {
        usleep(5);
    }

    for(int i = 0; i < nthread; i++) {
        pthread_join(pid[i], NULL);
    }

    assert(db->Close() == MBError::SUCCESS);
    delete db;

    Lookup();

    mabain::DB::CloseLogFile();
    SetTestStatus(true);
    return 0;
}
