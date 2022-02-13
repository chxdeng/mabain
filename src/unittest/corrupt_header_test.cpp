#include <string>
#include <thread>
#include <iostream>
#include <fstream>

#include <gtest/gtest.h>

#include "../db.h"
#include "../resource_pool.h"
#include "../drm_base.h"
#include "../version.h"

using namespace mabain;

namespace {

#define MB_DIR "/var/tmp/mabain_test/"

class CorruptHeaderTest : public ::testing::Test
{
public:
    CorruptHeaderTest() {
    }
    virtual ~CorruptHeaderTest() {
    }
    virtual void SetUp() {
        std::string cmd = std::string("rm ") + MB_DIR + "_*";
        if(system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown() {
        ResourcePool::getInstance().RemoveAll();
    }
    void CreateZeroHeaderFile(bool set_ver) {
        char hdr[RollableFile::page_size];
        IndexHeader *ptr = reinterpret_cast<IndexHeader*>(hdr);
        memset(hdr, 0, sizeof(hdr));
        if (set_ver) {
            ptr->version[0] = version[0];
            ptr->version[1] = version[1];
            ptr->version[2] = version[2];
        }
        std::string hdr_path = std::string(MB_DIR) + "_mabain_h";
        std::ofstream hdrf(hdr_path.c_str(), std::ios::out | std::ios::binary);
        hdrf.write(hdr, sizeof(hdr));
        hdrf.close();
    }
    void CreateInvalidHeaderFile(bool set_ver) {
        char hdr[RollableFile::page_size];
        for (auto i = 0; i < RollableFile::page_size; i++) {
            hdr[i] = (char) (i % 256);
        } 
        if (set_ver) {
            IndexHeader *ptr = reinterpret_cast<IndexHeader*>(hdr);
            ptr->version[0] = version[0];
            ptr->version[1] = version[1];
            ptr->version[2] = version[2];
        }
        std::string hdr_path = std::string(MB_DIR) + "_mabain_h";
        std::ofstream hdrf(hdr_path.c_str(), std::ios::out | std::ios::binary);
        hdrf.write(hdr, sizeof(hdr));
        hdrf.close();
    }
    void CreateRandomHeaderFile(bool set_ver) {
        srand(time(NULL));
        char hdr[RollableFile::page_size];
        for (auto i = 0; i < RollableFile::page_size; i++) {
            hdr[i] = rand() % 256;
        }
        if (set_ver) {
            IndexHeader *ptr = reinterpret_cast<IndexHeader*>(hdr);
            ptr->version[0] = version[0];
            ptr->version[1] = version[1];
            ptr->version[2] = version[2];
        }
        std::string hdr_path = std::string(MB_DIR) + "_mabain_h";
        std::ofstream hdrf(hdr_path.c_str(), std::ios::out | std::ios::binary);
        hdrf.write(hdr, sizeof(hdr));
        hdrf.close();
    }
protected:
};

TEST_F(CorruptHeaderTest, test_zero_hdr)
{
    CreateZeroHeaderFile(true);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_zero_hdr_1)
{
    CreateZeroHeaderFile(false);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_zero_hdr_2)
{
    CreateZeroHeaderFile(true);
    int options = CONSTS::ReaderOptions();
    DB db(MB_DIR, options);
    EXPECT_FALSE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_invalid_hdr)
{
    CreateInvalidHeaderFile(true);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_invalid_hdr_1)
{
    CreateInvalidHeaderFile(false);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_invalid_hdr_2)
{
    CreateInvalidHeaderFile(false);
    int options = CONSTS::ReaderOptions();
    DB db(MB_DIR, options);
    EXPECT_FALSE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_random_hdr)
{
    CreateRandomHeaderFile(true);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_random_hdr_1)
{
    CreateRandomHeaderFile(false);
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());
}

TEST_F(CorruptHeaderTest, test_random_hdr_2)
{
    CreateRandomHeaderFile(true);
    int options = CONSTS::ReaderOptions();
    DB db(MB_DIR, options);
    EXPECT_FALSE(db.is_open());
}

}
