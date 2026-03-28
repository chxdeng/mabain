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

#include <iostream>
#include <string>

#include "./jemalloc_rebuild_test_modes.h"

namespace {

int PrintUsage(const char* prog)
{
    std::cerr << "Usage: " << prog << " <mode>\n";
    std::cerr << "Supported modes:\n";
    for (const char* mode : mabain_test::kJemallocRebuildTestModes) {
        std::cerr << "  " << mode << "\n";
    }
    std::cerr << "Step 1 only wires the harness; no rebuild behavior is implemented yet.\n";
    return 2;
}

int RunScaffoldMode(const std::string& mode)
{
    std::cout << "jemalloc_restart_rebuild_test: scaffold-only mode '" << mode << "'\n";
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

    return RunScaffoldMode(mode);
}
