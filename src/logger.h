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

#ifndef __MB_LOGGER_H__
#define __MB_LOGGER_H__

#include <fstream>
#include <string>

namespace mabain {

#define LOG_LEVEL_ERROR   0
#define LOG_LEVEL_WARN    1
#define LOG_LEVEL_INFO    2
#define LOG_LEVEL_DEBUG   3

class Logger
{
public:
    ~Logger();

    // Logs a message
    static void Log(int level, const std::string &message);
    // Variable length log
    static void Log(int level, const char *formt, ... );
    static void FillDateTime(char *buffer, int bufsize);
    static void InitLogFile(const std::string &logfile);
    // Set log level. Default is logging everything except for debug.
    static int SetLogLevel(int level);
    static void Close();
    static std::ofstream* GetLogStream();

private:
    // This is a singleton class. Make sure nobody can create an instance.
    Logger();
    static void Rotate();

    static std::string log_file;
    static std::ofstream *log_stream;
    static const char* LOG_LEVEL[4];
    static int log_level_;
    static int roll_size;
};

}

#endif
