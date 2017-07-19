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

// A mabain command-line client

#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <string> 
#include <assert.h>
#include <signal.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "db.h"
#include "mb_data.h"
#include "error.h"
#include "util/mb_lsq.h"
#include "version.h"

using namespace mabain;

enum mbc_command {
    COMMAND_NONE = 0,
    COMMAND_QUIT = 1,
    COMMAND_UNKNOWN = 2,
    COMMAND_STATS = 3,
    COMMAND_FIND = 4,
    COMMAND_FIND_ALL = 5,
    COMMAND_INSERT = 6,
    COMMAND_REPLACE = 7,
    COMMAND_DELETE = 8,
    COMMAND_DELETE_ALL = 9,
    COMMAND_HELP = 10,
    COMMAND_RESET_N_WRITER = 11,
    COMMAND_RESET_N_READER = 12,
    COMMAND_FIND_LPREFIX = 13,
};

static DB *db = NULL;

static void HandleSignal(int sig)
{
    switch(sig)
    {
        case SIGSEGV:
            std::cerr << "process segfault\n";
        case SIGTERM:
        case SIGINT:
        case SIGQUIT:
        case SIGHUP:
        case SIGPIPE:
            if(db)
            {
                db->Close();
                delete db;
                db = NULL;
            } 
            exit(1);
        case SIGUSR1:
        case SIGUSR2:
            break;
    }
}

static void usage(const char *prog)
{
    std::cout << "Usage: " << prog << " -d mabain-directory [-im index-memcap] [-dm data-memcap] [-w]\n";
    std::cout <<"\t-d mabain databse directory\n";
    std::cout <<"\t-im index memcap\n";
    std::cout <<"\t-dm data memcap\n";
    std::cout <<"\t-w running in writer mode\n";
    exit(1);
}

static void show_help()
{
    std::cout << "\tfind(\"key\")\t\tsearch entry by key\n";
    std::cout << "\tfindPrefix(\"key\")\t\tsearch entry by key using longest prefix match\n";
    std::cout << "\tfindAll\t\t\tlist all entries\n";
    std::cout << "\tinsert(\"key\":\"value\")\tinsert a key-value pair\n";
    std::cout << "\treplace(\"key\":\"value\")\treplace a key-value pair\n";
    std::cout << "\tdelete(\"key\")\t\tdelete entry by key\n";
    std::cout << "\tdeleteAll\t\tdelete all entries\n";
    std::cout << "\tshow\t\t\tshow database statistics\n";
    std::cout << "\thelp\t\t\tshow helps\n";
    std::cout << "\tquit\t\t\tquit mabain client\n";
    std::cout << "\tclearWriterCheck\tClear writer count in shared memory header\n";
    std::cout << "\tdecReaderCount\t\tdecrement reader count in shared memory header\n";
}

static void trim_spaces(std::string &cmd, std::string &cmd_trim)
{
    cmd_trim.clear();

    bool quotation = false;
    for( std::string::iterator it=cmd.begin(); it!=cmd.end(); ++it)
    {
        if(isspace(*it))
        {
            if(!quotation)
                continue;
        }
        else if(*it == '\"')
        {
            quotation ^= true;
        }

        cmd_trim.append(1, *it);
    }
}

static int parse_command(const std::string &cmd,
                         std::string &key,
                         MBData &mbd)
{
    std::string yes;
    int insert_key_len = 0;
    switch(cmd[0])
    {
        case 'q':
            if(cmd.compare("quit") == 0)
                return COMMAND_QUIT;
            break;
        case 's':
            if(cmd.compare("show") == 0)
                return COMMAND_STATS;
            break;
        case 'f':
            if(cmd.compare(0, 6, "find(\"") == 0)
            {
                size_t end = cmd.find('\"', 6);
                if(end == std::string::npos)
                    return COMMAND_UNKNOWN;

                key = cmd.substr(6, end-6);
                return COMMAND_FIND;
            }
            else if(cmd.compare(0, 12, "findPrefix(\"") == 0)
            {
                size_t end = cmd.find('\"', 12);
                if(end == std::string::npos)
                    return COMMAND_UNKNOWN;

                key = cmd.substr(12, end-12);
                return COMMAND_FIND_LPREFIX;
            }
            else if(cmd.compare("findAll") == 0)
                return COMMAND_FIND_ALL;
            break;
        case 'd':
            if(cmd.compare(0, 8, "delete(\"") == 0)
            {
                size_t end = cmd.find('\"', 8);
                if(end == std::string::npos)
                    return COMMAND_UNKNOWN;

                key = cmd.substr(8, end-8);
                return COMMAND_DELETE;
            }
            else if(cmd.compare("deleteAll") == 0)
            {
                std::cout << "Do you want to delete all entries? Press \'y\' to continue: ";
                std::string del_all;
                std::getline(std::cin, del_all);
                if(del_all.length() == 0 || del_all[0] != 'y')
                    return COMMAND_NONE;
                return COMMAND_DELETE_ALL;
            }
            else if(cmd.compare("decReaderCount") == 0)
            {
                std::cout << "Do you want to decrement number of reader? Press \'y\' to continue: ";
                std::getline(std::cin, yes);   
                if(yes.length() > 0 && yes[0] == 'y')
                    return COMMAND_RESET_N_READER;
                return COMMAND_NONE;
            }
            break;
        case 'i':
            if(cmd.compare(0, 8, "insert(\"") == 0)
                insert_key_len = 8;
            break;
        case 'r':
            if(cmd.compare(0, 9, "replace(\"") == 0)
                insert_key_len = 9;
            break;
        case 'h':
            if(cmd.compare("help") == 0)
                return COMMAND_HELP;
            break;
        case 'c':
            if(cmd.compare("clearWriterCheck") == 0)
            {
                std::cout << "Do you want to reset number of writer? Press \'y\' to continue: ";
                std::getline(std::cin, yes);   
                if(yes.length() > 0 && yes[0] == 'y')
                    return COMMAND_RESET_N_WRITER;
                return COMMAND_NONE;
            }
            break;
        default:
            break;
    }

    if(insert_key_len > 0)
    {
        size_t end = cmd.find('\"', insert_key_len);
        if(end == std::string::npos)
            return COMMAND_UNKNOWN;
        key = cmd.substr(insert_key_len, end-insert_key_len);

        size_t start = cmd.find(":\"", end);
        if(start == std::string::npos)
            return COMMAND_UNKNOWN;
        end = cmd.find('\"', start+2);
        if(end == std::string::npos)
            return COMMAND_UNKNOWN;
        std::string value = cmd.substr(start+2, end-start-2);
        int len = value.length();
        mbd.Resize(len);
        memcpy(mbd.buff, value.c_str(), len);
        mbd.data_len = len;
        
        if(insert_key_len == 8)
            return COMMAND_INSERT;
        return COMMAND_REPLACE;
    }

    return COMMAND_UNKNOWN;
}

#define ENTRY_PER_PAGE 20
static void display_all_kvs()
{
    if(db == NULL)
        return;

    int count = 0;
    for(DB::iterator iter = db->begin(); iter != db->end(); ++iter)
    {
        count++;
        std::cout << iter.key << ": " <<
                     std::string((char *)iter.value.buff, iter.value.data_len) << "\n";
        if(count % ENTRY_PER_PAGE == 0)
        {
            std::string show_more;
            std::cout << "Press \'y\' for displaying more: ";
            std::getline(std::cin, show_more);
            if(show_more.length() == 0 || show_more[0] != 'y')
                break;
        }
    }
}

static void mbclient(const char *db_dir, int64_t memcap_i, int64_t memcap_d, int mode)
{
    rl_bind_key('\t', rl_complete);

    printf("mabain %d.%d.%d shell\n", version[0], version[1], version[2]);
    std::cout << "database directory: " << db_dir << "\nindex memcap: " << memcap_i <<
                 "\t data memcap: " << memcap_d << "\n";

    int rval;
    int cmd_id;
    std::string key;
    std::string value;
    std::string cmd;
    MBData mbd;
    bool quit_mbc = false;
    bool overwrite;
    MBlsq history(free);

    db = new DB(db_dir, memcap_i, memcap_d, mode);
    if(!db->is_open())
    {
        std::cout << db->StatusStr() << "\n";
        exit(1);
    }

    while(true)
    {
        std::string line = readline(">> ");
        if(quit_mbc) break;

        trim_spaces(line, cmd);
        if(cmd.length() == 0) continue;

        overwrite = false;
        mbd.Clear();
        cmd_id = parse_command(cmd, key, mbd);
        switch(cmd_id)
        {
            case COMMAND_NONE:
                // no opertation needed
                break;
            case COMMAND_QUIT:
                std::cout << "bye\n";
                quit_mbc = true;
                break;
            case COMMAND_FIND:
                rval = db->Find(key.c_str(), key.length(), mbd);
                if(rval == MBError::SUCCESS)
                {
                    std::cout << std::string((char *)mbd.buff, mbd.data_len);
                    std::cout << "\n";
                }
                else
                    std::cout << MBError::get_error_str(rval) << "\n";
                break;
            case COMMAND_FIND_LPREFIX:
                rval = db->FindLongestPrefix(key.c_str(), key.length(), mbd);
                if(rval == MBError::SUCCESS)
                {
                    std::cout << "longest prefix key matched: " << std::string(key.c_str(), mbd.match_len)
                              << ":" << std::string((char *)mbd.buff, mbd.data_len);
                    std::cout << "\n";
                }
                else
                    std::cout << MBError::get_error_str(rval) << "\n";
                break;
            case COMMAND_DELETE:
                if(mode & ACCESS_MODE_WRITER)
                {
                    mbd.options |= OPTION_FIND_AND_DELETE;
                    rval = db->Remove(key.c_str(), key.length(), mbd);
                    std::cout << MBError::get_error_str(rval) << "\n";
                }
                else
                    std::cout << "permission not allowed\n";
                break;
            case COMMAND_REPLACE:
                overwrite = true;
            case COMMAND_INSERT:
                if(mode & ACCESS_MODE_WRITER)
                {
                    rval = db->Add(key.c_str(), key.length(), mbd, overwrite);
                    std::cout << MBError::get_error_str(rval) << "\n";
                }
                else
                    std::cout << "permission not allowed\n";
                break;
            case COMMAND_STATS:
                db->PrintStats();
                break;
            case COMMAND_HELP:
                show_help();
                break;
            case COMMAND_DELETE_ALL:
                if(mode & ACCESS_MODE_WRITER)
                {
                    rval = db->RemoveAll();
                    std::cout << MBError::get_error_str(rval) << "\n";
                }
                else
                    std::cout << "permission not allowed\n";
                break;
            case COMMAND_FIND_ALL:
                if(mode & ACCESS_MODE_WRITER)
                    display_all_kvs();
                else
                    std::cout << "permission not allowed\n";
                break;
            case COMMAND_RESET_N_WRITER:
                db->UpdateNumHandlers(ACCESS_MODE_WRITER, -1);
                break;
            case COMMAND_RESET_N_READER:
                db->UpdateNumHandlers(ACCESS_MODE_READER, -1);
                break;
            case COMMAND_UNKNOWN:
            default:
                std::cout << "unknown query\n";
                break;
        }

        if(quit_mbc) break;
        add_history(line.c_str());
    }

    db->Close();
    delete db;
}

int main(int argc, char *argv[])
{
    sigset_t mask;

    signal(SIGINT, HandleSignal);
    signal(SIGTERM, HandleSignal);
    signal(SIGQUIT, HandleSignal);
    signal(SIGPIPE, HandleSignal);
    signal(SIGHUP, HandleSignal);
    signal(SIGSEGV, HandleSignal);
    signal(SIGUSR1, HandleSignal);
    signal(SIGUSR2, HandleSignal);

    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGQUIT);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGPIPE);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGSEGV);
    sigaddset(&mask, SIGUSR1);
    sigaddset(&mask, SIGUSR2);
    pthread_sigmask(SIG_SETMASK, &mask, NULL);

    sigemptyset(&mask);
    pthread_sigmask(SIG_SETMASK, &mask, NULL);

    int64_t memcap_i = 1024*1024LL;
    int64_t memcap_d = 1024*1024LL;
    const char *db_dir = NULL;
    int mode = 0;

    for(int i = 1; i < argc; i++)
    {
        if(strcmp(argv[i], "-d") == 0)
        {
            if(++i >= argc)
                usage(argv[0]);
            db_dir = argv[i];
        }
        else if(strcmp(argv[i], "-im") == 0)
        {
            if(++i >= argc)
                usage(argv[0]);
            memcap_i = atoi(argv[i]);
        }
        else if(strcmp(argv[i], "-dm") == 0)
        {
            if(++i >= argc)
                usage(argv[0]);
            memcap_d = atoi(argv[i]);
        }
        else if(strcmp(argv[i], "-w") == 0)
        {
            mode |= ACCESS_MODE_WRITER;
        }
        else
            usage(argv[0]);
    }

    if(db_dir == NULL)
        usage(argv[0]);

    mbclient(db_dir, memcap_i, memcap_d, mode);

    return 0;
}
