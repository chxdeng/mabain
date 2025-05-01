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

// @author Changxue Deng <chadeng@cisco.com>

#include "drm_base.h"
#include "error.h"
#include "resource_pool.h"
#include "util/utils.h"
#include "version.h"

namespace mabain {

void DRMBase::ReadHeaderVersion(const std::string& header_path, uint16_t ver[4])
{
    memset(ver, 0, sizeof(uint16_t) * 4);
    FILE* hdr_file = fopen(header_path.c_str(), "rb");
    if (hdr_file == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to open header file %s", header_path.c_str());
        throw (int)MBError::OPEN_FAILURE;
    }
    if (fread(ver, sizeof(uint16_t), 4, hdr_file) != 4) {
        fclose(hdr_file);
        throw (int)MBError::READ_ERROR;
    }
    fclose(hdr_file);
}

void DRMBase::ReadHeader(const std::string& header_path, uint8_t* buff, int buf_size)
{
    FILE* hdr_file = fopen(header_path.c_str(), "rb");
    if (hdr_file == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to open header file %s", header_path.c_str());
        throw (int)MBError::OPEN_FAILURE;
    }
    if (fread(buff, buf_size, 1, hdr_file) != 1) {
        fclose(hdr_file);
        throw (int)MBError::READ_ERROR;
    }
    fclose(hdr_file);
}

void DRMBase::WriteHeader(const std::string& header_path, uint8_t* buff)
{
    FILE* hdr_file = fopen(header_path.c_str(), "wb");
    if (hdr_file == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to open header file %s", header_path.c_str());
        throw (int)MBError::OPEN_FAILURE;
    }
    if (fwrite(buff, RollableFile::page_size, 1, hdr_file) != 1) {
        fclose(hdr_file);
        throw (int)MBError::WRITE_ERROR;
    }
    fclose(hdr_file);
}

void DRMBase::ValidateHeaderFile(const std::string& header_path, int mode,
    int queue_size, bool& update_header)
{
    uint16_t hdr_ver[4];
    ReadHeaderVersion(header_path, hdr_ver);
    if (hdr_ver[0] >= 1 && hdr_ver[1] >= 3)
        return;
    if (!(mode & CONSTS::ACCESS_MODE_WRITER))
        throw (int)MBError::VERSION_MISMATCH;

    Logger::Log(LOG_LEVEL_INFO, "header version: %u.%u.%u does not match "
                                "library version: %u.%u.%u",
        hdr_ver[0], hdr_ver[1], hdr_ver[2],
        version[0], version[1], version[2]);
    // Load the old header first
    uint8_t buff[RollableFile::page_size];
    ReadHeader(header_path, buff, RollableFile::page_size);
    // Update version field
    uint16_t* curr_version = reinterpret_cast<uint16_t*>(buff);
    memcpy(curr_version, version, 4 * sizeof(uint16_t));

    // Write the new header file
    std::string tmp_header_path = header_path + ".tmp";
    Logger::Log(LOG_LEVEL_INFO, "updating header to newer version %s", tmp_header_path.c_str());
    WriteHeader(tmp_header_path, buff);

    // Set up inode number
    bool map_hdr = true;
    std::shared_ptr<MmapFileIO> hdr_file;
    hdr_file = ResourcePool::getInstance().OpenFile(tmp_header_path,
        mode,
        RollableFile::page_size,
        map_hdr,
        false);
    if (hdr_file == NULL || !map_hdr)
        throw (int)MBError::OPEN_FAILURE;
    IndexHeader* hdr = reinterpret_cast<IndexHeader*>(hdr_file->GetMapAddr());
    hdr->shm_queue_id = get_file_inode(tmp_header_path);
    hdr->async_queue_size = queue_size;
    Logger::Log(LOG_LEVEL_INFO, "setting shm queue id to be %ld", hdr->shm_queue_id);
    hdr_file->Flush();
    ResourcePool::getInstance().RemoveResourceByPath(tmp_header_path);

    // Swap header files
    std::string old_header_path = header_path + "_" + std::to_string(hdr_ver[0]) + "_"
        + std::to_string(hdr_ver[1]) + "_" + std::to_string(hdr_ver[2]);
    if (rename(header_path.c_str(), old_header_path.c_str()) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to move file %s to %s", header_path.c_str(),
            old_header_path.c_str());
        throw (int)MBError::OPEN_FAILURE;
    }
    if (rename(tmp_header_path.c_str(), header_path.c_str()) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to move file %s to %s", tmp_header_path.c_str(),
            header_path.c_str());
        throw (int)MBError::OPEN_FAILURE;
    }

    update_header = true;
}

void DRMBase::PrintHeader(std::ostream& out_stream) const
{
    if (header == NULL)
        return;

    out_stream << "---------------- START OF HEADER ----------------" << std::endl;
    out_stream << "version: " << header->version[0] << "." << header->version[1] << "." << header->version[2] << std::endl;
    out_stream << "data size: " << header->data_size << std::endl;
    out_stream << "db count: " << header->count << std::endl;
    out_stream << "max data offset: " << header->m_data_offset << std::endl;
    out_stream << "max index offset: " << header->m_index_offset << std::endl;
    out_stream << "pending data buffer size: " << header->pending_data_buff_size << std::endl;
    out_stream << "pending index buffer size: " << header->pending_index_buff_size << std::endl;
    out_stream << "node count: " << header->n_states << std::endl;
    out_stream << "edge count: " << header->n_edges << std::endl;
    out_stream << "edge string size: " << header->edge_str_size << std::endl;
    out_stream << "writer count: " << header->num_writer << std::endl;
    out_stream << "reader count: " << header->num_reader << std::endl;
    out_stream << "shm queue ID: " << header->shm_queue_id << std::endl;
    out_stream << "writer option: " << header->writer_options << std::endl;
    out_stream << "data block size: " << header->data_block_size << std::endl;
    out_stream << "index block size: " << header->index_block_size << std::endl;
    out_stream << "lock free data: " << std::endl;
    out_stream << "\tcounter: " << header->lock_free.counter << std::endl;
    out_stream << "\toffset: " << header->lock_free.offset << std::endl;
    out_stream << "number of updates: " << header->num_update << std::endl;
    out_stream << "entry count per bucket: " << header->entry_per_bucket << std::endl;
    out_stream << "eviction bucket index: " << header->eviction_bucket_index << std::endl;
    out_stream << "exception data: " << std::endl;
    out_stream << "\tupdating status: " << header->excep_updating_status << std::endl;
    out_stream << "\texception data buffer: ";
    char data_str_buff[MB_EXCEPTION_BUFF_SIZE * 3 + 1];
    for (int i = 0; i < MB_EXCEPTION_BUFF_SIZE; i++) {
        sprintf(data_str_buff + 3 * i, "%2x ", header->excep_buff[i]);
    }
    data_str_buff[MB_EXCEPTION_BUFF_SIZE * 3] = '\0';
    out_stream << data_str_buff << std::endl;
    out_stream << "\toffset: " << header->excep_offset << std::endl;
    out_stream << "\tlock free offset: " << header->excep_lf_offset << std::endl;
    out_stream << "max index offset before rc: " << header->rc_m_index_off_pre << std::endl;
    out_stream << "max data offset before rc: " << header->rc_m_data_off_pre << std::endl;
    out_stream << "rc root offset: " << header->rc_root_offset << std::endl;
    out_stream << "rc count: " << header->rc_count << std::endl;
    out_stream << "shared memory queue size: " << header->async_queue_size << std::endl;
    out_stream << "shared memory queue index: " << header->queue_index << std::endl;
    out_stream << "shared memory writer index: " << header->writer_index << std::endl;
    out_stream << "resource flag: " << header->rc_flag << std::endl;
    out_stream << "---------------- END OF HEADER ----------------" << std::endl;
}

}
