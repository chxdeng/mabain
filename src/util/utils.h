#ifndef __UTILS_H__
#define __UTILS_H__

#include <string>

namespace mabain {

int  acquire_writer_lock(const std::string &lock_file_path);
void release_writer_lock(int fd);

}

#endif
