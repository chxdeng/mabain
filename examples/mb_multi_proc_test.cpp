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

// Multiple writers/readers access the DB concurrently.

#include <assert.h>
#include <iostream>
#include <map>
#include <openssl/sha.h>
#include <string.h>
#include <string>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include <mabain/db.h>

#define ADD_TEST 0
#define REMOVE_ONE_BY_ONE 1
#define REMOVE_ALL 2
#define SHRINK_TEST 3
#define LOOKUP_TEST 4
#define ITERATOR_TEST 5
#define KEY_TYPE_INT 0
#define KEY_TYPE_SHA256 1

const char* mb_dir = "/var/tmp/mabain_test";

using namespace mabain;

static int stop_processing = 0;
static int num_keys = 1000000;
static int test_type = 0;
static int64_t memcap_index = 256 * 1024 * 1024LL;
static int64_t memcap_data = 256 * 1024 * 1024LL;
static int key_type = KEY_TYPE_SHA256;
//static int key_type = KEY_TYPE_INT;
static std::map<std::string, int> checked;

static const char* get_sha256_str(int key);

static void SetTestStatus(bool success, bool stop_test = true)
{
    std::string cmd;
    if (success) {
        cmd = std::string("touch ") + mb_dir + "/_success";
    } else {
        cmd = std::string("rm ") + mb_dir + "/_success";
    }
    if (system(cmd.c_str()) != 0) {
    }
    if (stop_test) {
        abort();
    }
}

static void RemoveRandom(int rm_cnt, DB& db)
{
    int cnt = 0;
    std::string kv;
    int ikey = 0;
    while (cnt <= rm_cnt) {
        cnt++;

        ikey = rand() % num_keys + 1;
        switch (key_type) {
        case KEY_TYPE_SHA256:
            kv = get_sha256_str(ikey);
            break;
        case KEY_TYPE_INT:
        default:
            kv = std::string("key") + std::to_string(ikey);
            break;
        }

        db.Remove(kv);
        checked[kv] = 0;
        if (cnt % 1000000 == 0)
            std::cout << "Removed " << cnt << "\n";
    }
}

static void PopulateDB(DB& db)
{
    int rval;
    int nkeys = 0;
    std::string kv;
    while (nkeys < num_keys) {
        nkeys++;

        switch (key_type) {
        case KEY_TYPE_SHA256:
            kv = get_sha256_str(nkeys);
            break;
        case KEY_TYPE_INT:
        default:
            kv = std::string("key") + std::to_string(nkeys);
            break;
        }

        rval = db.Add(kv, kv);
        if (rval != MBError::SUCCESS) {
            std::cout << "failed to add " << kv << " " << rval << "\n";
            SetTestStatus(false);
        }
        checked[kv] = 1;

        if (nkeys % 1000000 == 0)
            std::cout << "populate db:  "
                      << " inserted " << nkeys << " keys\n";
    }
}

static void RemoveHalf(DB& db)
{
    int rval;
    int nkeys = 0;
    std::string kv;
    while (nkeys < num_keys) {
        nkeys++;
        if (nkeys % 1000000 == 0)
            std::cout << "Removed " << nkeys / 2 << "\n";
        if (nkeys % 2 == 0)
            continue;

        switch (key_type) {
        case KEY_TYPE_SHA256:
            kv = get_sha256_str(nkeys);
            break;
        case KEY_TYPE_INT:
        default:
            kv = std::string("key") + std::to_string(nkeys);
            break;
        }
        rval = db.Remove(kv.c_str(), kv.length());
        checked[kv] = 0;
        if (rval != MBError::SUCCESS) {
            std::cout << "failed to remove " << nkeys << " " << kv << " " << rval << "\n";
            SetTestStatus(false);
        }
    }
}

//Writer process
static void Writer(int id)
{
    DB db(mb_dir, CONSTS::WriterOptions(), memcap_index, memcap_data);
    if (!db.is_open()) {
        std::cerr << "writer " << id << " failed to open mabain db: "
                  << db.StatusStr() << "\n";
        exit(1);
    }

    if (test_type == SHRINK_TEST) {
        db.CollectResource(0, 0);

        db.Close();
        exit(0);
    }

    int rval;
    int nkeys = 0;
    std::string kv;
    int count = 0;
    while (!stop_processing) {
        nkeys++;

        switch (key_type) {
        case KEY_TYPE_SHA256:
            kv = get_sha256_str(nkeys);
            break;
        case KEY_TYPE_INT:
        default:
            kv = std::string("key") + std::to_string(nkeys);
            break;
        }

        if (test_type == REMOVE_ONE_BY_ONE) {
            rval = db.Remove(kv.c_str(), kv.length());
            if (rval != MBError::SUCCESS && rval != MBError::NOT_EXIST) {
                std::cout << "failed to remove " << kv << "\n";
                SetTestStatus(false);
            } else {
                count++;
            }
        } else if (test_type == REMOVE_ALL) {
            if (nkeys == int(num_keys * 0.666666)) {
                db.RemoveAll();
            }
        } else {
            rval = db.Add(kv, kv);
            if (rval != MBError::SUCCESS && rval != MBError::IN_DICT) {
                std::cout << "failed to add " << kv << " " << rval << "\n";
                SetTestStatus(false);
            } else if (rval == MBError::SUCCESS) {
                count++;
            }
        }

        if (nkeys % 1000000 == 0)
            std::cout << "writer " << id << " looped over " << nkeys << " keys\n";
        if (nkeys >= num_keys)
            break;
    }

    db.Close();
    exit(0);
}

//Reader process
static void Reader(int id)
{
    DB db(mb_dir, CONSTS::ReaderOptions(), memcap_index, memcap_data);
    if (!db.is_open()) {
        std::cerr << "reader " << id << " failed to open mabain db: "
                  << db.StatusStr() << "\n";
        SetTestStatus(false);
    }

    std::string key;
    if (test_type == ITERATOR_TEST) {
        int count = 0;
        for (DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
            count++;
            if (iter.key != std::string((char*)iter.value.buff, iter.value.data_len)) {
                std::cout << "VALUE NOT MATCH " << iter.key << ": "
                          << std::string((char*)iter.value.buff, iter.value.data_len) << "\n";
                SetTestStatus(false);
            } else {
                if (checked[iter.key] == 1)
                    checked[iter.key]++;
            }
        }
        db.Close();
        for (int i = 1; i <= num_keys; i++) {
            switch (key_type) {
            case KEY_TYPE_SHA256:
                key = get_sha256_str(i);
                break;
            case KEY_TYPE_INT:
            default:
                key = std::string("key") + std::to_string(i);
                break;
            }
            if (!(checked[key] == 0 || checked[key] == 2)) {
                std::cerr << i << ": " << key << " " << checked[key] << " for id " << id << "\n";
                SetTestStatus(false);
            }
        }
        exit(0);
    }

    int ikey = 1;
    MBData mb_data;
    int rval;
    int nfound = 0;
    while (!stop_processing) {
        switch (key_type) {
        case KEY_TYPE_SHA256:
            key = get_sha256_str(ikey);
            break;
        case KEY_TYPE_INT:
        default:
            key = std::string("key") + std::to_string(ikey);
            break;
        }
        rval = db.Find(key, mb_data);

        if (rval == MBError::SUCCESS) {
            // check the value read from DB
            if (memcmp(key.c_str(), mb_data.buff, mb_data.data_len)) {
                std::cout << "READER " << id << " VALUE DOES NOT MATCH: "
                          << key << ":" << mb_data.match_len << " "
                          << std::string((const char*)mb_data.buff, mb_data.data_len) << "\n";
                SetTestStatus(false);
            }

            ikey++;
            nfound++;
        } else if (rval == MBError::NOT_EXIST) {
            ikey++;
        } else {
            std::cerr << "ERROR: " << MBError::get_error_str(rval) << "\n";
            SetTestStatus(false);
        }

        if (ikey % 1000000 == 0)
            std::cout << "reader " << id << " looked up " << ikey << " keys\n";

        if (ikey > num_keys)
            break;
    }

    stop_processing = 1;
    db.Close();
    exit(0);
}

static char sha256_str[65];
static const char* get_sha256_str(int key)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, (unsigned char*)&key, 4);
    SHA256_Final(hash, &sha256);
    int i = 0;
    for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(sha256_str + (i * 2), "%02x", hash[i]);
    }
    sha256_str[64] = 0;
    return (const char*)sha256_str;
}

int main(int argc, char* argv[])
{
    int num_writer = 1; // There can be one writer at most.
    int num_reader = 2;
    test_type = -1;
    srand(time(NULL));

    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    if (argc >= 4) {
        const char* test_tp = argv[1];
        if (strcmp(test_tp, "add") == 0)
            test_type = ADD_TEST;
        else if (strcmp(test_tp, "remove") == 0)
            test_type = REMOVE_ONE_BY_ONE;
        else if (strcmp(test_tp, "remove-all") == 0)
            test_type = REMOVE_ALL;
        else if (strcmp(test_tp, "shrink") == 0)
            test_type = SHRINK_TEST;
        else if (strcmp(test_tp, "lookup") == 0)
            test_type = LOOKUP_TEST;
        else if (strcmp(test_tp, "iterator") == 0)
            test_type = ITERATOR_TEST;
        num_keys = atoi(argv[2]);
        num_reader = atoi(argv[3]);
    }
    if (argc >= 5) {
        mb_dir = argv[4];
    }

    SetTestStatus(false, false);
    if (test_type < 0) {
        test_type = ADD_TEST;
    }
    // Delete all keys from last run.
    DB db(mb_dir, CONSTS::WriterOptions(), memcap_index, memcap_data);
    if (!db.is_open()) {
        std::cerr << " failed to open mabain db: "
                  << db.StatusStr() << "\n";
        exit(1);
    }

    db.RemoveAll();
    PopulateDB(db);
    switch (test_type) {
    case ADD_TEST:
        RemoveRandom(int(num_keys * 0.7777777), db);
        break;
    case LOOKUP_TEST:
        break;
    case SHRINK_TEST:
        RemoveHalf(db);
        break;
    case REMOVE_ONE_BY_ONE:
        break;
    case REMOVE_ALL:
        break;
    case ITERATOR_TEST:
        RemoveRandom(int(num_keys * 0.666666), db);
        break;
    default:
        abort();
    }

    db.Close();

    std::vector<pid_t> pid_arr;
    pid_t test_pid = getpid();

    if (test_type == LOOKUP_TEST)
        num_writer = 0;
    assert(num_writer < 2);
    struct timeval start_tm;
    gettimeofday(&start_tm, NULL);
    for (int i = 0; i < num_writer; i++) {
        if (getpid() != test_pid)
            continue;

        pid_t pid = fork();
        if (pid < 0)
            break;

        if (getpid() == test_pid) {
            pid_arr.push_back(pid);
            continue;
        }

        // Start child/writer process
        Writer(i);
    }

    for (int i = 0; i < num_reader; i++) {
        if (getpid() != test_pid)
            continue;

        pid_t pid = fork();
        if (pid < 0)
            break;

        if (getpid() == test_pid) {
            pid_arr.push_back(pid);
            continue;
        }

        // Start child/reader process
        Reader(i);
    }

    if (test_pid == getpid()) {
        int status;
        int i;
        while (!stop_processing) {
            // Check if all children exited
            for (i = 0; i < (int)pid_arr.size(); i++) {
                if (pid_arr[i] > 0) {
                    waitpid(pid_arr[i], &status, WUNTRACED | WCONTINUED);
                    pid_arr[i] = -1;

                    if (i == (int)pid_arr.size() - 1) {
                        stop_processing = 1;
                        struct timeval stop_tm;
                        gettimeofday(&stop_tm, NULL);
                        //std::cout << "All children have exited!\n";
                        //std::cout << "time: " << ((stop_tm.tv_sec-start_tm.tv_sec)*1000000.0 +
                        //            (stop_tm.tv_usec-start_tm.tv_usec))/num_keys << "\n";
                    }
                }
            }

            usleep(1000);
        }
    }

    mabain::DB::CloseLogFile();
    SetTestStatus(true, false);
    return 0;
}
