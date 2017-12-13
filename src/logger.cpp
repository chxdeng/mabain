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

#include <stdarg.h>
#include <iostream>
#include <unistd.h>

#include "logger.h"
#include "error.h"

#define MAX_NUM_LOG 10

namespace mabain {

std::string Logger::log_file = "";
std::ofstream* Logger::log_stream = NULL;
int Logger::log_level_       = LOG_LEVEL_WARN;
int Logger::roll_size        = 50*1024*1024;
const char* Logger::LOG_LEVEL[4] =
{
    " ERROR: ",
    " WARN: ",
    " INFO: ",
    " DEBUG: "
};

Logger::Logger()
{
}

Logger::~Logger()
{
}

void Logger::Close()
{
    if(log_stream != NULL)
    {
        if(log_stream->is_open())
            log_stream->close();

        delete log_stream;
        log_stream = NULL;
    }
}

// Should only be called by writer for now.
// Readers will send logs to stdout or stderr.
void Logger::InitLogFile(const std::string &logfile)
{
    if(!logfile.empty())
    {
        log_file = logfile;
        log_stream = new std::ofstream();
        log_stream->open(log_file.c_str(), std::ios::out | std::ios::app);
    }
}

void Logger::FillDateTime(char *buffer, int bufsize)
{
    time_t rawtime;
    struct tm timeinfo;
    time (&rawtime);
    if(localtime_r(&rawtime, &timeinfo))
        strftime(buffer, bufsize, "%Y-%m-%d.%X", &timeinfo);
    else
        snprintf(buffer, bufsize, "Time unknown");
}

void Logger::Rotate()
{
    if(log_stream == NULL)
        return;
    if(log_stream->is_open())
        log_stream->close();

    std::string filepath_old;
    std::string filepath_new;
    for(int i = MAX_NUM_LOG-2; i > 0; i--)
    {
        filepath_old = log_file + "." + std::to_string(i);
        if(access(filepath_old.c_str(), R_OK) == 0)
        {
            filepath_new = log_file + "." + std::to_string(i+1);
            if(rename(filepath_old.c_str(), filepath_new.c_str()))
                std::cerr << "failed to move log file\n";
        }
    }
    filepath_new = log_file + ".1";
    if(rename(log_file.c_str(), filepath_new.c_str()))
        std::cerr << "failed to move log file\n";

    log_stream->open(log_file.c_str(), std::ios::out | std::ios::app);
}

void Logger::Log(int level, const std::string &message)
{
    if(level > log_level_)
        return;

    char buffer[80];
    FillDateTime(buffer, sizeof(buffer));
    if(log_stream != NULL)
    {
        *log_stream << buffer << LOG_LEVEL[level] << message << std::endl;
        if(log_stream->tellp() > roll_size)
            Logger::Rotate();
    }
    else if(level < LOG_LEVEL_INFO)
        std::cerr << buffer << ": " << message << std::endl;
    else
        std::cout << buffer << ": " << message << std::endl;
}

void Logger::Log(int level, const char *format, ... )
{
    if(level > log_level_)
        return;

    char message[256];
    char buffer[64];
    FillDateTime(buffer, sizeof(buffer));
    va_list args;
    va_start(args, format);
    vsprintf(message, format, args);
    if(log_stream != NULL)
    {
        *log_stream << buffer << LOG_LEVEL[level] << message << std::endl;
        if(log_stream->tellp() > roll_size)
            Logger::Rotate();
    }
    else if(level < LOG_LEVEL_INFO)
        std::cerr << buffer << ": " << message << std::endl;
    else
        std::cout << buffer << ": " << message << std::endl;
    va_end(args);
}

int Logger::SetLogLevel(int level)
{
    if(level < 0 || level > LOG_LEVEL_DEBUG)
    {
        Logger::Log(LOG_LEVEL_WARN, "invaid logging level %d", level);
        return MBError::INVALID_ARG;
    }

    log_level_ = level;

    return MBError::SUCCESS;
}

std::ofstream* Logger::GetLogStream()
{
    return log_stream;
}

}
