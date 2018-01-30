/**
 * Copyright (C) 2018 Cisco Inc.
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

// @author Shridhar Bhalerao <shbhaler@cisco.com>

#include <iostream>
#include <fstream>
#include <limits.h>
#include <sstream>

#include "mb_backup.h"
#include "mb_data.h"
#include "integer_4b_5b.h"

namespace mabain {

DBBackup::DBBackup(const DB &db) : db_ref(db)
{
    if(!(db.GetDBOptions() & CONSTS::ACCESS_MODE_WRITER))
        throw (int) MBError::NOT_ALLOWED;

    Dict *dict = db_ref.GetDictPtr();
    if(dict == NULL)
        throw (int) MBError::NOT_INITIALIZED;
    
    header = dict->GetHeaderPtr();
    if(header == NULL)
        throw (int) MBError::NOT_INITIALIZED;

}

DBBackup::~DBBackup()
{
}


void DBBackup::copy_file (const std::string &src_path, const std::string &dest_path,
                          char *buffer, int buffer_size)
{
    FILE *fp_read, *fp_write;

    if ((fp_read = fopen (src_path.c_str(),"rb")) == NULL)
    {
        Logger::Log(LOG_LEVEL_ERROR, "Backup failed: Could not open file %s", src_path.c_str()); 
        throw MBError::OPEN_FAILURE;
    }
    if ((fp_write = fopen(dest_path.c_str(),"wb")) == NULL)
    {
        fclose(fp_read);
        Logger::Log(LOG_LEVEL_ERROR, "Backup failed: Could not open file %s", dest_path.c_str()); 
        throw MBError::OPEN_FAILURE;
    }
    
    int rval = MBError::SUCCESS;
    int read_count, write_count;

    while(true)
    {
        read_count = fread(buffer, buffer_size, 1, fp_read);

        if(feof(fp_read) && read_count == 0)
             break;
        if (read_count != 1)
        {
            rval = MBError::READ_ERROR;
            break;
        }

        write_count = fwrite(buffer,buffer_size, 1, fp_write);
        if(write_count != 1)
        {
            rval = MBError::WRITE_ERROR;
            break;
        }
    }
    fclose(fp_read);
    fclose(fp_write);
    
    if(rval != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "Backup failed %s", MBError::get_error_str(rval)); 
        throw rval;
    }
}

int DBBackup::Backup(const char * bk_dir)
{
    if(bk_dir == NULL)
        throw (int) MBError::INVALID_ARG;

    std::string bk_header_path = std::string(bk_dir) + "/_mabain_h";
    if(access(bk_header_path.c_str(), R_OK) == 0)
        throw (int) MBError::OPEN_FAILURE;
    
    if(!db_ref.is_open())
        throw (int) db_ref.Status();
   
    if(header->m_data_offset > MAX_6B_OFFSET || header->m_index_offset > MAX_6B_OFFSET)
        throw (int) MBError::INVALID_SIZE;
    if(header->data_block_size == 0 || header->data_block_size % BLOCK_SIZE_ALIGN != 0)
        throw (int) MBError::INVALID_SIZE;
    if(header->index_block_size == 0 || header->index_block_size % BLOCK_SIZE_ALIGN != 0)
        throw (int) MBError::INVALID_SIZE;
    
    db_ref.Flush();

    int num_data_files, num_index_files;
    // get the number of files from the header: max_bytes/number_of_bytes_per_file
    num_data_files = (header->m_data_offset/header->data_block_size) + 1; 
    num_index_files = (header->m_index_offset/header->index_block_size) + 1;
    
    char *buffer = (char *) malloc(BLOCK_SIZE_ALIGN);
    if(buffer == NULL)
        throw (int) MBError::NO_MEMORY;
    
    // loop through all data files
    const std::string &orig_dir = db_ref.GetDBDir();
    std::string read_file_path_base = orig_dir + "/_mabain_d";
    std::string write_file_path_base = std::string(bk_dir) + "/_mabain_d";
    std::string read_file_path;
    std::string write_file_path;
    
    for (int i = 0; i < num_data_files; i++)
    {
        read_file_path = read_file_path_base + std::to_string(i);
        write_file_path = write_file_path_base + std::to_string(i);
        copy_file(read_file_path, write_file_path, buffer, BLOCK_SIZE_ALIGN);
    }
    
    read_file_path_base = orig_dir + "/_mabain_i";
    write_file_path_base = std::string(bk_dir) + "/_mabain_i";
    for (int i = 0; i < num_index_files; i++)
    {
        read_file_path = read_file_path_base + std::to_string(i);
        write_file_path = write_file_path_base + std::to_string(i);
        copy_file(read_file_path, write_file_path, buffer, BLOCK_SIZE_ALIGN);
    }
    
    read_file_path = orig_dir + "/_mabain_h";
    write_file_path = std::string(bk_dir) + "/_mabain_h";
    //header is size of page_size
    copy_file(read_file_path, write_file_path, buffer, RollableFile::page_size);
    
    free(buffer);
    
    //reset number readers/writers in backed up DB.
    int rval;
    DB db = DB(bk_dir, CONSTS::ACCESS_MODE_READER, 0, 0);
    rval = db.UpdateNumHandlers(CONSTS::ACCESS_MODE_WRITER, -1);
    if(rval != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_WARN,"failed to reset number of writer for DB %s", bk_dir);
    
    rval = db.UpdateNumHandlers(CONSTS::ACCESS_MODE_READER, INT_MIN);
    if(rval != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_WARN,"failed to reset number of writer for DB %s", bk_dir);
    db.Close();
    return MBError::SUCCESS;
}

}
