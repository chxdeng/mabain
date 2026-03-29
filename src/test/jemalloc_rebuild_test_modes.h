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

#ifndef MABAIN_JEMALLOC_REBUILD_TEST_MODES_H_
#define MABAIN_JEMALLOC_REBUILD_TEST_MODES_H_

#include <algorithm>
#include <array>
#include <string>

namespace mabain_test {

constexpr std::array<const char*, 9> kJemallocRebuildTestModes = {
    "header_metadata",
    "arena_cursor",
    "startup_gate",
    "async_reject",
    "shrink_only",
    "evacuate_only",
    "recover_shrink",
    "recover_evacuate",
    "full_cycle",
};

inline bool IsJemallocRebuildTestModeSupported(const std::string& mode)
{
    return std::find(kJemallocRebuildTestModes.begin(),
               kJemallocRebuildTestModes.end(),
               mode) != kJemallocRebuildTestModes.end();
}

} // namespace mabain_test

#endif
