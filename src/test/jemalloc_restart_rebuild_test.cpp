/**
 * Copyright (C) 2026 Cisco Inc.
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

#include <cstdlib>
#include <iostream>
#include <string>

#include "../error.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"
#include "../rollable_file.h"
#include "./jemalloc_rebuild_test_modes.h"

namespace {

constexpr char kArenaCursorTestDir[] = "/var/tmp/mabain_test/jemalloc_rebuild";
constexpr size_t kArenaCursorBlockSize = 4 * 1024 * 1024;
constexpr size_t kArenaCursorMemCap = 4 * kArenaCursorBlockSize;

int PrintUsage(const char* prog)
{
    std::cerr << "Usage: " << prog << " <mode>\n";
    std::cerr << "Supported modes:\n";
    for (const char* mode : mabain_test::kJemallocRebuildTestModes) {
        std::cerr << "  " << mode << "\n";
    }
    std::cerr << "Only modes implemented for the current step perform real validation.\n";
    return 2;
}

int RunScaffoldMode(const std::string& mode)
{
    std::cout << "jemalloc_restart_rebuild_test: scaffold-only mode '" << mode << "'\n";
    return 0;
}

int PrepareArenaCursorDir()
{
    if (system("mkdir -p /var/tmp/mabain_test") != 0) {
        return 1;
    }
    if (system("rm -rf /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
        return 1;
    }
    if (system("mkdir -p /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
        return 1;
    }
    return 0;
}

int VerifyArenaCursor(const std::string& path)
{
    mabain::RollableFile file(path, kArenaCursorBlockSize, kArenaCursorMemCap,
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, 4);

    if (file.PreAlloc(128) == nullptr) {
        std::cerr << "failed PreAlloc for " << path << "\n";
        return 1;
    }
    if (file.GetJemallocAllocSize() != 128u) {
        std::cerr << "unexpected initial alloc_size for " << path << "\n";
        return 1;
    }

    if (file.ResetJemalloc() != mabain::MBError::SUCCESS) {
        std::cerr << "ResetJemalloc failed for " << path << "\n";
        return 1;
    }

    const size_t reseed_offset = kArenaCursorBlockSize + 321;
    if (file.ReseedJemalloc(reseed_offset) != mabain::MBError::SUCCESS) {
        std::cerr << "ReseedJemalloc failed for " << path << "\n";
        return 1;
    }
    if (file.GetJemallocAllocSize() != reseed_offset) {
        std::cerr << "unexpected reseeded alloc_size for " << path << "\n";
        return 1;
    }

    size_t offset1 = 0;
    void* ptr1 = file.Malloc(128, offset1);
    if (ptr1 == nullptr || offset1 < reseed_offset) {
        std::cerr << "first allocation failed after reseed for " << path << "\n";
        return 1;
    }
    const size_t alloc_size1 = file.GetJemallocAllocSize();

    if (file.ResetJemalloc() != mabain::MBError::SUCCESS ||
        file.ReseedJemalloc(reseed_offset) != mabain::MBError::SUCCESS) {
        std::cerr << "reset+reseed replay failed for " << path << "\n";
        return 1;
    }

    size_t offset2 = 0;
    void* ptr2 = file.Malloc(128, offset2);
    if (ptr2 == nullptr || offset2 != offset1 || file.GetJemallocAllocSize() != alloc_size1) {
        std::cerr << "reset+reseed was not deterministic for " << path << "\n";
        return 1;
    }

    return 0;
}

int RunArenaCursorMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare arena_cursor test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();

    const int index_rc = VerifyArenaCursor(std::string(kArenaCursorTestDir) + "/_mabain_i");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (index_rc != 0) {
        return index_rc;
    }

    const int data_rc = VerifyArenaCursor(std::string(kArenaCursorTestDir) + "/_mabain_d");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (data_rc != 0) {
        return data_rc;
    }

    std::cout << "jemalloc_restart_rebuild_test: arena_cursor passed\n";
    return 0;
}

} // namespace

int main(int argc, char* argv[])
{
    if (argc != 2) {
        return PrintUsage(argv[0]);
    }

    std::string mode(argv[1]);
    if (!mabain_test::IsJemallocRebuildTestModeSupported(mode)) {
        return PrintUsage(argv[0]);
    }

    if (mode == "arena_cursor") {
        return RunArenaCursorMode();
    }

    return RunScaffoldMode(mode);
}
