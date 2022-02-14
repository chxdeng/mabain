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

#ifndef __MABAIN_H__
#define __MABAIN_H__

namespace mabain {

class CONSTS
{
public:
    static const int ACCESS_MODE_READER;
    static const int ACCESS_MODE_WRITER;
    static const int ASYNC_WRITER_MODE;
    static const int SYNC_ON_WRITE;
    static const int USE_SLIDING_WINDOW;
    static const int MEMORY_ONLY_MODE;
    static const int READ_ONLY_DB;

    static const int OPTION_ALL_PREFIX;
    static const int OPTION_FIND_AND_STORE_PARENT;
    static const int OPTION_RC_MODE;
    static const int OPTION_READ_SAVED_EDGE; // Used internally only
    static const int OPTION_INTERNAL_NODE_BOUND;
    static const int OPTION_ADD_OVERWRITE;
    static const int OPTION_APPEND;
    static const int OPTION_APPEND_LINK_KEY;

    static const int MAX_KEY_LENGHTH;
    static const int MAX_DATA_SIZE;

    static int WriterOptions();
    static int ReaderOptions();
};

}

#endif
