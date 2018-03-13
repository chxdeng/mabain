
#include <stdio.h>

#include <unistd.h>
#include <stdlib.h>
#include <list>
#include <cstdlib>
#include <iostream>
#include <gtest/gtest.h>
#include <time.h>

#include "../db.h"
#include "../mb_data.h"
#include "./test_key.h"
#include "./mb_backup.h"

#define MB_DIR "/var/tmp/mabain_test/"
#define MB_BACKUP_DIR "/var/tmp/mabain_backup/"
#define MB_BACKUP_DIR_2 "/var/tmp/mabain_backup_2/"

using namespace mabain;

namespace {

class BackupTest : public ::testing::Test
{
public:
    BackupTest() {
    }
    virtual ~BackupTest() {
    }
    virtual void SetUp()
    {
        std::string cmd_2 = std::string("mkdir -p ") + MB_DIR;
        std::string cmd = std::string("mkdir -p ") + MB_BACKUP_DIR;
        if(system(cmd_2.c_str())!= 0){
        }
        if(system(cmd.c_str()) != 0) {
        }
        cmd = std::string("rm ") + MB_DIR + "_*";
        std::string cmd_1 = std::string("rm ") + MB_BACKUP_DIR + "_*";
        if(system(cmd.c_str()) != 0){
        }
        if(system(cmd_1.c_str()) != 0){
        }
        cmd = std::string("mkdir -p ") + MB_BACKUP_DIR_2;
        if(system(cmd.c_str()) != 0){
        }
        cmd = std::string("rm  ") + MB_BACKUP_DIR_2 + "_*";
        if(system(cmd.c_str()) != 0){
        }

    }
    virtual void TearDown() {

    }

    void check_overwritten_keys(DB *db_bkp, int num)
    {
        TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
        TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
        std::string key;
        int rval;
        for(int i = 0; i < num; i++) 
        {
            MBData mbd;
            key = tkey.get_key(i);
            rval = db_bkp->Find(key, mbd);
            EXPECT_EQ(rval, MBError::SUCCESS);
            EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len)==key+"_new", true);
            key = tkey1.get_key(i);
            rval = db_bkp->Find(key, mbd);
            EXPECT_EQ(rval, MBError::SUCCESS);
            EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len)==key+"_new", true);
        }
    }


protected:
};


TEST_F(BackupTest, Create_backup_db)
{
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
    int num = 10;
    std::string key;
    int rval;
    DB *db = new DB(MB_DIR, CONSTS::WriterOptions());
    assert(db->is_open());
    for(int i = 0; i < num; i++) 
    {
        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }

    clock_t t;
    t = clock();

    try 
    {
        DBBackup bk(*db);
        rval = bk.Backup(MB_BACKUP_DIR);
    }
    catch (int error)
    {
        std::cout << "Backup failed "<< MBError::get_error_str(error) <<"\n";
        rval = error;

    }
    EXPECT_EQ(rval, MBError::SUCCESS);
    t = clock() -t;
    printf ("It took me %ld clicks (%f seconds).\n",t,((float)t)/CLOCKS_PER_SEC);
    DB *db_bkp = new DB(MB_BACKUP_DIR, CONSTS::WriterOptions());

    //Test to check already inserted key.
    for(int i = 0; i < num; i++) 
    {
        key = tkey.get_key(i);
        rval = db_bkp->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);
        key = tkey1.get_key(i);
        rval = db_bkp->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);
    }

    //Test to overwrite existing key and retrive it.
    for(int i = 0; i < num; i++)
    {
        key = tkey.get_key(i);
        rval = db_bkp->Add(key, key+"_new", true);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db_bkp->Add(key, key+"_new", true);
    }

    // Retrive the overwritten key and check it.
    check_overwritten_keys(db_bkp, num);

    db_bkp->Close();
    delete db_bkp;
    //Asycwriter DB backup test
    db_bkp = new DB(MB_BACKUP_DIR, CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE, 128LL*1024*1024, 128LL*1024*1024);
    assert(db_bkp->is_open());
    assert(db_bkp->AsyncWriterEnabled());
    DB *db_r = new DB(MB_BACKUP_DIR, CONSTS::ReaderOptions(), 128LL*1024*1024, 128LL*1024*1024);
    assert(db_r->SetAsyncWriterPtr(db_bkp) == MBError::SUCCESS);
    assert(db_r->AsyncWriterEnabled());
    rval = db_r->Backup(MB_BACKUP_DIR_2);
    EXPECT_EQ(rval, MBError::SUCCESS);
    while (db_r->AsyncWriterBusy())
        usleep(10);
    assert(db_r->UnsetAsyncWriterPtr(db_bkp) == MBError::SUCCESS);
    db_bkp->Close();
    db_r->Close();
    delete db_bkp;
    delete db_r;
    db_bkp = new DB(MB_BACKUP_DIR_2, CONSTS::WriterOptions(), 128LL*1024*1024, 128LL*1024*1024);
    check_overwritten_keys(db_bkp, num);

    //Test to remove all the keys from the DB.
    rval = db_bkp->RemoveAll();
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(0, db_bkp->Count());
    db_bkp->Close();
    delete db_bkp;

    db->Close();
    delete db;
}
}
