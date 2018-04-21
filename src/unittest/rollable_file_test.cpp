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

#include <string>

#include <gtest/gtest.h>

#include "../rollable_file.h"
#include "../mabain_consts.h"
#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

#define ROLLABLE_FILE_TEST_DIR "/var/tmp/mabain_test"
#define FAKE_DATA "dsklckk sldk&&sdijds8990s9090230290399&&^^%%sdhsjdhsjdhsjxnmzn  lkvlsdlq;';'a;b; ;;slv; ;;;sdfl; lls;lf;sld;sld;sld;sll;skl;klk;gk;akl;s"
#define ONE_MEGA 1024*1024ul

class RollableFileTest : public ::testing::Test
{
public:
    RollableFileTest() {
        rfile = NULL;
    }
    virtual ~RollableFileTest() {
        if(rfile != NULL)
            delete rfile;
    }

    virtual void SetUp() {
        std::string cmd = std::string("mkdir -p ") + ROLLABLE_FILE_TEST_DIR;
        if(system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown() {
        ResourcePool::getInstance().RemoveAll();
        std::string cmd = std::string("rm -rf ") + ROLLABLE_FILE_TEST_DIR + "/_*";
        if(system(cmd.c_str()) != 0) {
        }
    }

    void Init() {
    }

protected:
    RollableFile *rfile;
};

TEST_F(RollableFileTest, constructor_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_d",
                4*ONE_MEGA, 4*ONE_MEGA, CONSTS::ACCESS_MODE_WRITER, 0);
    EXPECT_EQ(rfile != NULL, true);
    delete rfile;
    ResourcePool::getInstance().RemoveAll();

    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_d",
                4*ONE_MEGA, 4*ONE_MEGA, CONSTS::ACCESS_MODE_READER, 2);
    EXPECT_EQ(rfile != NULL, true);
}

TEST_F(RollableFileTest, RandomWrite_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_d",
                4*ONE_MEGA, 4*ONE_MEGA, CONSTS::ACCESS_MODE_WRITER, 0);
    
    EXPECT_EQ(rfile != NULL, true);

    size_t nbytes;
    size_t offset;
    uint8_t *ptr;

    nbytes = 5;
    offset = 0;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 5u);

    uint8_t buff[256];
    rfile->RandomRead(buff, nbytes, offset);
    EXPECT_EQ(memcmp(buff, FAKE_DATA, nbytes)==0, true);

    nbytes = 78;
    offset = ONE_MEGA + 28372;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 78u);
    rfile->RandomRead(buff, nbytes, offset);
    EXPECT_EQ(memcmp(buff, FAKE_DATA, nbytes)==0, true);
}

TEST_F(RollableFileTest, RandomRead_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_i",
                4*ONE_MEGA, 4*ONE_MEGA,
                CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW, 0);
    
    EXPECT_EQ(rfile != NULL, true);
    std::atomic<size_t> sliding_addr;
    rfile->InitShmSlidingAddr(&sliding_addr);

    size_t nbytes;
    size_t offset;
    uint8_t *ptr;

    nbytes = 15;
    offset = 20;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 15u);

    uint8_t buff[256];
    rfile->RandomRead(buff, nbytes, offset);
    EXPECT_EQ(memcmp(buff, FAKE_DATA, nbytes)==0, true);

    nbytes = 35;
    offset = 8*ONE_MEGA + 28372;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 35u);
    rfile->RandomRead(buff, nbytes, offset);
    EXPECT_EQ(memcmp(buff, FAKE_DATA, nbytes)==0, true);
}

TEST_F(RollableFileTest, Reserve_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_i",
                4*ONE_MEGA, 4*ONE_MEGA,
                CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW, 0);
    EXPECT_EQ(rfile != NULL, true);

    size_t offset;
    int size;
    uint8_t *ptr = NULL;
    int rval;

    offset = 0;
    size = 34; 
    rval = rfile->Reserve(offset, size, ptr, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 0u);
    EXPECT_EQ(ptr != NULL, true);

    offset = 12123;
    size = 19; 
    rval = rfile->Reserve(offset, size, ptr, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 12123u);
    EXPECT_EQ(ptr != NULL, true);
}

TEST_F(RollableFileTest, GetShmPtr_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_i",
                4*ONE_MEGA, 4*ONE_MEGA,
                CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW, 0);
    EXPECT_EQ(rfile != NULL, true);

    size_t offset;
    int size;
    uint8_t *ptr = NULL;
    int rval;

    offset = 42321;
    size = 49; 
    rval = rfile->Reserve(offset, size, ptr, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 42321u);
    EXPECT_EQ(ptr != NULL, true);
    EXPECT_EQ(ptr == rfile->GetShmPtr(offset, size), true);
}

TEST_F(RollableFileTest, CheckAlignment_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_i",
                4*ONE_MEGA, 4*ONE_MEGA, CONSTS::ACCESS_MODE_WRITER, 0);
    EXPECT_EQ(rfile != NULL, true);

    size_t offset;
    int size;

    offset = 1001;
    size = 23;
    offset = rfile->CheckAlignment(offset, size);
    EXPECT_EQ(offset, 1001u);

    offset = 4*ONE_MEGA-12;
    size = 55;
    offset = rfile->CheckAlignment(offset, size);
    EXPECT_EQ(offset, 4*ONE_MEGA);
}

TEST_F(RollableFileTest, Flush_test)
{
    rfile = new RollableFile(std::string(ROLLABLE_FILE_TEST_DIR) + "/_mabain_i",
                4*ONE_MEGA, 4*ONE_MEGA,
                CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW, 0);
    EXPECT_EQ(rfile != NULL, true);
    std::atomic<size_t> sliding_addr;
    rfile->InitShmSlidingAddr(&sliding_addr);
    
    int nbytes = 78;
    size_t offset = ONE_MEGA + 28372;
    uint8_t *ptr;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 78);
    offset = 4*ONE_MEGA + 233232;
    nbytes = 101;
    rfile->Reserve(offset, nbytes, ptr);
    nbytes = rfile->RandomWrite((const void *)FAKE_DATA, nbytes, offset);
    EXPECT_EQ(nbytes, 101);
    rfile->Flush();
}

}
