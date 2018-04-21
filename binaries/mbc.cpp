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
#include <fstream>
#include <string> 
#include <assert.h>
#include <signal.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "db.h"
#include "mb_data.h"
#include "dict.h"
#include "error.h"
#include "version.h"

#include "hexbin.h"
#include "expr_parser.h"

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
    COMMAND_PRINT_HEADER = 14,
    COMMAND_FIND_HEX = 15,
    COMMAND_FIND_LPREFIX_HEX = 16,
    COMMAND_RECLAIM_RESOURCES = 17,
    COMMAND_PARSING_ERROR = 18,
};

volatile bool quit_mbc = false;
static void HandleSignal(int sig)
{
    switch(sig)
    {
        case SIGSEGV:
            std::cerr << "process segfault\n";
            abort();
        case SIGTERM:
        case SIGINT:
        case SIGQUIT:
        case SIGHUP:
        case SIGPIPE:
            quit_mbc = true;
        case SIGUSR1:
        case SIGUSR2:
            break;
    }
}

static void usage(const char *prog)
{
    std::cout << "Usage: " << prog << " -d mabain-directory [-im index-memcap] [-dm data-memcap] [-w] [-e query] [-s script-file]\n";
    std::cout <<"\t-d mabain databse directory\n";
    std::cout <<"\t-im index memcap\n";
    std::cout <<"\t-dm data memcap\n";
    std::cout <<"\t-w running in writer mode\n";
    std::cout <<"\t-e run query on command line\n";
    std::cout <<"\t-s run queries in a file\n";
    exit(1);
}

static void show_help()
{
    std::cout << "\tfind(\"key\")\t\tsearch entry by key\n";
    std::cout << "\tfindPrefix(\"key\")\tsearch entry by key using longest prefix match\n";
    std::cout << "\tfindAll\t\t\tlist all entries\n";
    std::cout << "\tinsert(\"key\":\"value\")\tinsert a key-value pair\n";
    std::cout << "\treplace(\"key\":\"value\")\treplace a key-value pair\n";
    std::cout << "\tdelete(\"key\")\t\tdelete entry by key\n";
    std::cout << "\tdeleteAll\t\tdelete all entries\n";
    std::cout << "\tshow\t\t\tshow database statistics\n";
    std::cout << "\thelp\t\t\tshow helps\n";
    std::cout << "\tquit\t\t\tquit mabain client\n";
    std::cout << "\tdecWriterCount\t\tClear writer count in shared memory header\n";
    std::cout << "\tdecReaderCount\t\tdecrement reader count in shared memory header\n";
    std::cout << "\tprintHeader\t\tPrint shared memory header\n";
    std::cout << "\treclaimResources\tReclaim deleted resources\n";
}

static void trim_spaces(const char *cmd, std::string &cmd_trim)
{
    cmd_trim.clear();

    int quotation = 0;
    const char *p = cmd;
    while(*p != '\0')
    {
        if(*p == '\'' || *p == '\"')
        {
            cmd_trim.append(1, '\'');
            quotation ^= 1; 
        }
        else if(!isspace(*p) || quotation) 
        {
            cmd_trim.append(1, *p);
        }

        p++;
    }
}

static bool check_hex_output(std::string &cmd)
{
    size_t pos = cmd.rfind(".hex()");
    if(pos == std::string::npos)
        return false;
    if(pos == cmd.length()-6)
    {
        cmd.erase(pos);
        return true;
    }

    return false;
}

static int parse_key_value_pair(const std::string &kv_str,
                                std::string &key,
                                std::string &value)
{
    // search for ':' that separates key and value pair
    // currently this utility does not support quotation in
    // quotation, e.g, "abc\"def" as key or value.
    size_t pos = 0;
    int quotation_cnt = 0;
    for(size_t i = 0; i < kv_str.length(); i++)
    {
        if(kv_str[i] == '\'')
        {
            quotation_cnt++;
        }
        else if(kv_str[i] == ':')
        {
            // do not count the ':' in the key or value string.
            if(quotation_cnt % 2 == 0)
            {
                pos = i;
                break;
            }
        }
    }

    if(pos == 0)
        return -1;

    ExprParser expr_key(kv_str.substr(0, pos));
    if(expr_key.Evaluate(key) < 0)
        return -1;

    ExprParser expr_value(kv_str.substr(pos+1));
    if(expr_value.Evaluate(value) < 0)
        return -1;

    return 0;
}

static int parse_command(std::string &cmd,
                         std::string &key,
                         std::string &value)
{
    std::string yes;
    bool hex_output = false;

    key = "";
    value = "";
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
            hex_output = check_hex_output(cmd);
            if(cmd.compare(0, 5, "find(") == 0)
            {
                if(cmd[cmd.length()-1] != ')')
                    return COMMAND_UNKNOWN;
                ExprParser expr(cmd.substr(5, cmd.length()-6));
                if(expr.Evaluate(key) < 0)
                    return COMMAND_PARSING_ERROR;
                if(hex_output)
                    return COMMAND_FIND_HEX;
                return COMMAND_FIND;
            }
            else if(cmd.compare(0, 11, "findPrefix(") == 0)
            {
                if(cmd[cmd.length()-1] != ')')
                    return COMMAND_UNKNOWN;
                ExprParser expr(cmd.substr(11, cmd.length()-12));
                if(expr.Evaluate(key) < 0)
                    return COMMAND_PARSING_ERROR;
                if(hex_output)
                    return COMMAND_FIND_LPREFIX_HEX;
                return COMMAND_FIND_LPREFIX;
            }
            else if(cmd.compare("findAll") == 0)
                return COMMAND_FIND_ALL;
            break;
        case 'd':
            if(cmd.compare(0, 7, "delete(") == 0)
            {
                if(cmd[cmd.length()-1] != ')')
                    return COMMAND_UNKNOWN;
                ExprParser expr(cmd.substr(7, cmd.length()-8));
                if(expr.Evaluate(key) < 0)
                    return COMMAND_PARSING_ERROR; 
                return COMMAND_DELETE;
            }
            else if(cmd.compare("deleteAll") == 0)
            {
                std::cout << "Do you want to delete all entries? Press \'Y\' to continue: ";
                std::string del_all;
                std::getline(std::cin, del_all);
                if(del_all.length() == 0 || del_all[0] != 'Y')
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
            else if(cmd.compare("decWriterCount") == 0)
            {
                std::cout << "Do you want to decrement number of reader? Press \'y\' to continue: ";
                std::getline(std::cin, yes);   
                if(yes.length() > 0 && yes[0] == 'y')
                    return COMMAND_RESET_N_WRITER;
                return COMMAND_NONE;
            }
            break;
        case 'i':
            if(cmd.compare(0, 7, "insert(") == 0)
            {
                if(cmd[cmd.length()-1] != ')')
                    return COMMAND_UNKNOWN;
                if(parse_key_value_pair(cmd.substr(7, cmd.length()-8), key, value) < 0)
                    return COMMAND_PARSING_ERROR;
                return COMMAND_INSERT;
            }
            break;
        case 'r':
            if(cmd.compare(0, 8, "replace(") == 0)
            {
                if(cmd[cmd.length()-1] != ')')
                    return COMMAND_UNKNOWN;
                if(parse_key_value_pair(cmd.substr(8, cmd.length()-9), key, value) < 0)
                    return COMMAND_PARSING_ERROR;
                return COMMAND_REPLACE;
            }
            else if(cmd.compare("reclaimResources") == 0)
                return COMMAND_RECLAIM_RESOURCES;
            else
                return COMMAND_UNKNOWN;
            break;
        case 'h':
            if(cmd.compare("help") == 0)
                return COMMAND_HELP;
            break;
        case 'p':
            if(cmd.compare("printHeader") == 0)
                return COMMAND_PRINT_HEADER;
            break;
        default:
            break;
    }

    return COMMAND_UNKNOWN;
}

#define ENTRY_PER_PAGE 20
static void display_all_kvs(DB *db)
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

static void display_output(const MBData &mbd, bool hex_output, bool prefix)
{
    if(prefix)
        std::cout << "key length matched: " << mbd.match_len << "\n";
    if(hex_output)
    {
        char hex_buff[256];
        int len = mbd.data_len;
        if(256 < 2*len + 1)
        {
            std::cout << "display the first 127 bytes\n";
            len = 127;
        }
        if(bin_2_hex((const uint8_t *)mbd.buff, len, hex_buff, 256) < 0)
            std::cout << "failed to convert binary to hex\n";
        else 
            std::cout << hex_buff << "\n";
    }
    else
    {
        std::cout << std::string((char *)mbd.buff, mbd.data_len) << "\n";
    }
}

static int RunCommand(int mode, DB *db, int cmd_id, const std::string &key, const std::string &value)
{
    int rval = MBError::SUCCESS;
    bool overwrite = false;
    bool hex_output = false;
    MBData mbd;

    switch(cmd_id)
    {
        case COMMAND_NONE:
            // no opertation needed
            break;
        case COMMAND_QUIT:
            std::cout << "bye\n";
            quit_mbc = true;
            break;
        case COMMAND_FIND_HEX:
            hex_output = true;
        case COMMAND_FIND:
            rval = db->Find(key, mbd);
            if(rval == MBError::SUCCESS)
                display_output(mbd, hex_output, false);
            else
                std::cout << MBError::get_error_str(rval) << "\n";
            break;
        case COMMAND_FIND_LPREFIX_HEX:
            hex_output = true;
        case COMMAND_FIND_LPREFIX:
            rval = db->FindLongestPrefix(key, mbd);
            if(rval == MBError::SUCCESS)
                display_output(mbd, hex_output, true);
            else
                std::cout << MBError::get_error_str(rval) << "\n";
            break;
        case COMMAND_DELETE:
            if(mode & CONSTS::ACCESS_MODE_WRITER)
            {
                rval = db->Remove(key);
                std::cout << MBError::get_error_str(rval) << "\n";
            }
            else
                std::cout << "permission not allowed\n";
            break;
        case COMMAND_REPLACE:
            overwrite = true;
        case COMMAND_INSERT:
            if(mode & CONSTS::ACCESS_MODE_WRITER)
            {
                rval = db->Add(key, value, overwrite);
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
            if(mode & CONSTS::ACCESS_MODE_WRITER)
            {
                rval = db->RemoveAll();
                std::cout << MBError::get_error_str(rval) << "\n";
            }
            else
                std::cout << "permission not allowed\n";
            break;
        case COMMAND_FIND_ALL:
            display_all_kvs(db);
            break;
        case COMMAND_RESET_N_WRITER:
            if(mode & CONSTS::ACCESS_MODE_WRITER)
                std::cout << "writer is running, cannot reset writer counter\n";
            else
                db->UpdateNumHandlers(CONSTS::ACCESS_MODE_WRITER, -1);
            break;
        case COMMAND_RESET_N_READER:
            db->UpdateNumHandlers(CONSTS::ACCESS_MODE_READER, -1);
            break;
        case COMMAND_PRINT_HEADER:
            db->PrintHeader();
            break;
        case COMMAND_RECLAIM_RESOURCES:
            if(mode & CONSTS::ACCESS_MODE_WRITER)
                db->CollectResource(1, 1);
            else
                std::cout << "writer is not running, can not perform grabage collection" << std::endl;
            break;
        case COMMAND_PARSING_ERROR:
            break;
        case COMMAND_UNKNOWN:
        default:
            std::cout << "unknown query\n";
            break;
    }

    return rval;
}

static void mbclient(DB *db, int mode)
{
    rl_bind_key('\t', rl_complete);

    printf("mabain %d.%d.%d shell\n", version[0], version[1], version[2]);
    std::cout << "database directory: " << db->GetDBDir() << "\n";

    int cmd_id;
    std::string key;
    std::string value;
    std::string cmd;

    while(true)
    {
        char* line = readline(">> ");
        if(line == NULL) break;
        if(line[0] == '\0')
        {
            free(line);
            continue;
        }

        trim_spaces(line, cmd);
        add_history(line);
        free(line);
        cmd_id = parse_command(cmd, key, value);

        RunCommand(mode, db, cmd_id, key, value);

        if(quit_mbc) break;
    }
}

static void run_query_command(DB *db, int mode, const std::string &command_str)
{
    std::string cmd;
    int cmd_id;
    std::string key;
    std::string value;

    trim_spaces(command_str.c_str(), cmd);
    if(cmd.length() == 0)
    {
        std::cerr << command_str << " not a valid command\n";
        return;
    }

    cmd_id = parse_command(cmd, key, value);
    RunCommand(mode, db, cmd_id, key, value);
}

static void run_script(DB *db, int mode, const std::string &script_file)
{
    std::ifstream script_in(script_file);
    if(!script_in.is_open()) {
        std::cerr << "cannot open file " << script_file << "\n";
        return;
    }

    std::string line;
    int cmd_id;
    std::string key;
    std::string value;
    std::string cmd;
    
    while(getline(script_in, line))
    {
        trim_spaces(line.c_str(), cmd);
        if(cmd.length() == 0)
        {
            std::cerr << line << " not a valid query\n";
            continue;
        }

        cmd_id = parse_command(cmd, key, value);
        std::cout << cmd << ": ";
        RunCommand(mode, db, cmd_id, key, value);

        if(quit_mbc) break;
    }
    script_in.close();
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
    std::string query_cmd = "";
    std::string script_file = "";

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
            mode |= CONSTS::ACCESS_MODE_WRITER;
        }
        else if(strcmp(argv[i], "-e") == 0)
        {
            if(++i >= argc)
                usage(argv[0]);
            query_cmd = argv[i];
        }
        else if(strcmp(argv[i], "-s") == 0)
        {
            if(++i >= argc)
                usage(argv[0]);
            script_file = argv[i];
        }
        else
            usage(argv[0]);
    }

    if(db_dir == NULL)
        usage(argv[0]);

    DB *db = new DB(db_dir, mode, memcap_i, memcap_d);
    if(!db->is_open())
    {
        std::cout << db->StatusStr() << "\n";
        exit(1);
    }

    // DB::SetLogFile("/var/tmp/mabain.log");
    // DB::LogDebug();

    if(query_cmd.length() != 0)
    {
        run_query_command(db, mode, query_cmd);
    }
    else if(script_file.length() != 0)
    {
        run_script(db, mode, script_file);
    }
    else
    {
        mbclient(db, mode);
    }

    db->Close();
    // DB::CloseLogFile();
    delete db;
    return 0;
}
