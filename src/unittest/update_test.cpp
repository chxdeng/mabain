#include <cstdlib>
#include <list>
#include <stdlib.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../dict_mem.h"
#include "../free_list.h"
#include "../mb_data.h"
#include "../resource_pool.h"
#include "./test_key.h"

#define MB_DIR "/var/tmp/mabain_test/"

using namespace mabain;

namespace {

class UpdateTest : public ::testing::Test {
public:
    UpdateTest()
    {
        db = NULL;
    }
    virtual ~UpdateTest()
    {
        if (db != NULL)
            delete db;
    }
    virtual void SetUp()
    {
        std::string cmd = std::string("mkdir -p ") + MB_DIR;
        if (system(cmd.c_str()) != 0) {
        }
        cmd = std::string("rm ") + MB_DIR + "_*";
        if (system(cmd.c_str()) != 0) {
        }
        db = new DB(MB_DIR, CONSTS::WriterOptions());
    }
    virtual void TearDown()
    {
        db->Close();
        ResourcePool::getInstance().RemoveAll();
    }

protected:
    DB* db;
};

TEST_F(UpdateTest, Update_all)
{
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
    int num = 1000;
    std::string key;
    int rval;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);

        key = tkey.get_key(i);
        rval = db->Add(key, key + "_new", true);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key + "_new", true);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }

    MBData mbd;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == key + "_new", true);
        key = tkey1.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == key + "_new", true);
    }
}

TEST_F(UpdateTest, FailedLeafRemoveKeepsPublishedValue)
{
    const std::string removed_key = "ab";
    const std::string original_value = "old-value";
    const std::string replacement_value = "new-value";

    ASSERT_EQ(db->Add("aa", "value-aa"), MBError::SUCCESS);
    ASSERT_EQ(db->Add(removed_key, original_value), MBError::SUCCESS);
    ASSERT_EQ(db->Add("ac", "value-ac"), MBError::SUCCESS);

    DictMem* dmm = db->GetDictPtr()->GetMM();
    ASSERT_NE(dmm, nullptr);
    ASSERT_NE(dmm->GetFreeList(), nullptr);
    dmm->GetFreeList()->Empty();
    dmm->SetReserveLimit(dmm->GetHeaderPtr()->m_index_offset);

    int remove_result = MBError::SUCCESS;
    try {
        remove_result = db->GetDictPtr()->Remove(
            reinterpret_cast<const uint8_t*>(removed_key.data()),
            removed_key.size());
    } catch (int error) {
        remove_result = error;
    }
    dmm->ClearReserveLimit();
    ASSERT_EQ(remove_result, MBError::OUT_OF_BOUND);

    // Force reuse of the released same-sized value buffer. A failed removal
    // must leave the original key/value pair intact.
    ASSERT_EQ(db->Add("z", replacement_value), MBError::SUCCESS);

    MBData found;
    ASSERT_EQ(db->Find(removed_key, found), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(found.buff), found.data_len),
        original_value);
}

TEST_F(UpdateTest, SynchronousRemoveConvertsInternalException)
{
    ASSERT_EQ(db->Add("aa", "value-aa"), MBError::SUCCESS);
    ASSERT_EQ(db->Add("ab", "value-ab"), MBError::SUCCESS);
    ASSERT_EQ(db->Add("ac", "value-ac"), MBError::SUCCESS);

    DictMem* dmm = db->GetDictPtr()->GetMM();
    ASSERT_NE(dmm, nullptr);
    ASSERT_NE(dmm->GetFreeList(), nullptr);
    dmm->GetFreeList()->Empty();
    dmm->SetReserveLimit(dmm->GetHeaderPtr()->m_index_offset);

    int remove_result = MBError::SUCCESS;
    EXPECT_NO_THROW(remove_result = db->Remove("ab", 2));
    dmm->ClearReserveLimit();

    EXPECT_EQ(remove_result, MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->Status(), MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->Add("z", "value-z"), MBError::NOT_INITIALIZED);
}

TEST_F(UpdateTest, FailedStructuralAddReleasesUnpublishedValue)
{
    ASSERT_EQ(db->Add("aa", "value-aa"), MBError::SUCCESS);
    ASSERT_EQ(db->Add("ab", "value-ab"), MBError::SUCCESS);
    ASSERT_EQ(db->Add("ac", "value-ac"), MBError::SUCCESS);

    DictMem* dmm = db->GetDictPtr()->GetMM();
    ASSERT_NE(dmm, nullptr);
    ASSERT_NE(dmm->GetFreeList(), nullptr);
    dmm->GetFreeList()->Empty();
    dmm->SetReserveLimit(dmm->GetHeaderPtr()->m_index_offset);

    const std::string failed_value = "failed-value";
    MBData failed_data;
    failed_data.buff = reinterpret_cast<uint8_t*>(
        const_cast<char*>(failed_value.data()));
    failed_data.data_len = failed_value.size();

    int add_result = MBError::SUCCESS;
    try {
        add_result = db->Add("ad", 2, failed_data);
    } catch (int error) {
        add_result = error;
    }
    dmm->ClearReserveLimit();
    ASSERT_EQ(add_result, MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->Status(), MBError::SUCCESS);
    const size_t failed_value_offset = failed_data.data_offset;
    failed_data.buff = nullptr;

    MBData missing;
    EXPECT_EQ(db->Find("ad", 2, missing), MBError::NOT_EXIST);

    // A failed structural add never publishes this value. The next allocation
    // of the same size should therefore reuse its released buffer.
    ASSERT_EQ(db->Add("z", failed_value), MBError::SUCCESS);
    MBData reused;
    ASSERT_EQ(db->Find("z", 1, reused), MBError::SUCCESS);
    EXPECT_EQ(reused.data_offset, failed_value_offset);
}

TEST_F(UpdateTest, FailedStructuralAddReleasesIntermediateIndexBuffers)
{
    const std::string existing_key = "abcdefghiX12345678";
    const std::string failed_key = "abcdefghiYabcdefgh";
    const std::string existing_value = "existing-value";
    const std::string failed_value = "failed-value";

    ASSERT_EQ(db->Add(existing_key, existing_value), MBError::SUCCESS);

    DictMem* dmm = db->GetDictPtr()->GetMM();
    ASSERT_NE(dmm, nullptr);
    FreeList* free_list = dmm->GetFreeList();
    ASSERT_NE(free_list, nullptr);
    free_list->Empty();

    IndexHeader* header = dmm->GetHeaderPtr();
    const int node_size = free_list->GetAlignmentSize(
        dmm->GetNodeSizePtr()[1]);
    const int split_edge_size = 8;
    const size_t node_index = free_list->GetBufferIndex(node_size);
    const size_t edge_index = free_list->GetBufferIndex(split_edge_size);
    ASSERT_NE(node_index, edge_index);

    const uint64_t states_before = header->n_states;
    const uint64_t edge_strings_before = header->edge_str_size;
    const uint64_t count_before = header->count;

    size_t limit = dmm->CheckAlignment(header->m_index_offset, node_size)
        + node_size;
    limit = dmm->CheckAlignment(limit, split_edge_size) + split_edge_size;
    limit = dmm->CheckAlignment(limit, split_edge_size) + split_edge_size;
    dmm->SetReserveLimit(limit);

    MBData failed_data;
    failed_data.buff = reinterpret_cast<uint8_t*>(
        const_cast<char*>(failed_value.data()));
    failed_data.data_len = failed_value.size();
    int add_result = MBError::SUCCESS;
    try {
        add_result = db->Add(failed_key.data(), failed_key.size(), failed_data);
    } catch (int error) {
        add_result = error;
    }
    dmm->ClearReserveLimit();
    failed_data.buff = nullptr;

    ASSERT_EQ(add_result, MBError::OUT_OF_BOUND);
    EXPECT_EQ(header->n_states, states_before);
    EXPECT_EQ(header->edge_str_size, edge_strings_before);
    EXPECT_EQ(header->count, count_before);
    EXPECT_EQ(free_list->GetBufferCountByIndex(node_index), 1u);
    EXPECT_EQ(free_list->GetBufferCountByIndex(edge_index), 2u);

    MBData found;
    ASSERT_EQ(db->Find(existing_key, found), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(found.buff), found.data_len),
        existing_value);
    EXPECT_EQ(db->Find(failed_key, found), MBError::NOT_EXIST);
}

TEST_F(UpdateTest, Update_random)
{
    srand(time(NULL));
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
    int num = 3456;
    std::string key;
    int rval;
    bool* added;

    added = new bool[num];
    for (int i = 0; i < num; i++) {
        added[i] = false;
    }

    for (int i = 0; i < num; i++) {
        if (rand() % 100 < 21)
            continue;

        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        added[i] = true;
    }
    for (int i = 0; i < num; i++) {
        if (added[i]) {
            key = tkey.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::IN_DICT);
            key = tkey1.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::IN_DICT);

            key = tkey.get_key(i);
            rval = db->Add(key, key + "_new", true);
            EXPECT_EQ(rval, MBError::SUCCESS);
            key = tkey1.get_key(i);
            rval = db->Add(key, key + "_new", true);
            EXPECT_EQ(rval, MBError::SUCCESS);
        } else {
            key = tkey.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::SUCCESS);
            key = tkey1.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::SUCCESS);
        }
    }

    MBData mbd;
    std::string value;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        value = key;
        if (added[i])
            value += "_new";
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == value, true);
        key = tkey1.get_key(i);
        rval = db->Find(key, mbd);
        value = key;
        if (added[i])
            value += "_new";
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == value, true);
    }

    delete[] added;
}

TEST_F(UpdateTest, LeafOverwriteDoesNotReuseLiveValueBuffer)
{
    const std::string key = "leaf-key";
    const std::string old_value(256, 'A');
    const std::string new_value(256, 'B');

    ASSERT_EQ(db->Add(key, old_value), MBError::SUCCESS);
    MBData before;
    ASSERT_EQ(db->Find(key, before), MBError::SUCCESS);
    const size_t old_offset = before.data_offset;

    ASSERT_EQ(db->Add(key, new_value, true), MBError::SUCCESS);
    MBData after;
    ASSERT_EQ(db->Find(key, after), MBError::SUCCESS);
    EXPECT_NE(after.data_offset, old_offset);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(after.buff), after.data_len),
        new_value);

    // The old buffer is released only after publication and remains reusable.
    const std::string reuse_key = "z";
    const std::string reuse_value(256, 'C');
    ASSERT_EQ(db->Add(reuse_key, reuse_value), MBError::SUCCESS);
    MBData reused;
    ASSERT_EQ(db->Find(reuse_key, reused), MBError::SUCCESS);
    EXPECT_EQ(reused.data_offset, old_offset);
}

TEST_F(UpdateTest, NodeValueOverwriteDoesNotReuseLiveValueBuffer)
{
    const std::string parent_key = "a";
    const std::string child_key = "ab";
    const std::string old_value(256, 'A');
    const std::string new_value(256, 'B');

    ASSERT_EQ(db->Add(parent_key, old_value), MBError::SUCCESS);
    ASSERT_EQ(db->Add(child_key, "child-value"), MBError::SUCCESS);

    MBData before;
    ASSERT_EQ(db->Find(parent_key, before), MBError::SUCCESS);
    const size_t old_offset = before.data_offset;

    ASSERT_EQ(db->Add(parent_key, new_value, true), MBError::SUCCESS);
    MBData after;
    ASSERT_EQ(db->Find(parent_key, after), MBError::SUCCESS);
    EXPECT_NE(after.data_offset, old_offset);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(after.buff), after.data_len),
        new_value);

    MBData child;
    ASSERT_EQ(db->Find(child_key, child), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(child.buff), child.data_len),
        "child-value");
}

TEST_F(UpdateTest, BorrowedValuePointerLookup)
{
    const std::string leaf_key = "borrowed-leaf";
    const std::string leaf_value = "borrowed-leaf-value";
    const std::string parent_key = "borrowed-parent";
    const std::string parent_value = "borrowed-parent-value";
    const std::string child_key = "borrowed-parent-child";

    ASSERT_EQ(db->Add(leaf_key, leaf_value), MBError::SUCCESS);
    ASSERT_EQ(db->Add(parent_key, parent_value), MBError::SUCCESS);
    ASSERT_EQ(db->Add(child_key, "child-value"), MBError::SUCCESS);

    MBData data(64, CONSTS::OPTION_RETURN_DATA_PTR);
    uint8_t* const owned_buffer = data.buff;

    ASSERT_EQ(db->Find(leaf_key, data), MBError::SUCCESS);
    ASSERT_NE(data.data_ptr, nullptr);
    EXPECT_EQ(data.buff, owned_buffer);
    EXPECT_EQ(data.data_ptr,
        db->GetDataPtrByOffset(data.data_offset + DB::GetDataHeaderSize()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data_ptr),
                  data.data_len),
        leaf_value);

    ASSERT_EQ(db->Find(parent_key, data), MBError::SUCCESS);
    ASSERT_NE(data.data_ptr, nullptr);
    EXPECT_EQ(data.buff, owned_buffer);
    EXPECT_EQ(data.data_ptr,
        db->GetDataPtrByOffset(data.data_offset + DB::GetDataHeaderSize()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data_ptr),
                  data.data_len),
        parent_value);

    EXPECT_EQ(db->ReadDataByOffset(
                  static_cast<size_t>(MAX_6B_OFFSET) + 1, data),
        MBError::READ_ERROR);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(data.data_len, 0);

    EXPECT_EQ(db->Find("missing-borrowed-key", data), MBError::NOT_EXIST);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(data.data_len, 0);

    ASSERT_EQ(db->Find(parent_key, data), MBError::SUCCESS);
    ASSERT_NE(data.data_ptr, nullptr);
    data.options = 0;
    EXPECT_EQ(db->Find("missing-after-mode-change", data), MBError::NOT_EXIST);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(data.data_len, 0);

    data.options = CONSTS::OPTION_RETURN_DATA_PTR;
    ASSERT_EQ(db->Find(parent_key, data), MBError::SUCCESS);
    ASSERT_NE(data.data_ptr, nullptr);
    data.options = 0;
    EXPECT_EQ(db->FindLongestPrefix("zzzz-no-prefix-match", data),
        MBError::NOT_EXIST);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(data.data_len, 0);

    ASSERT_EQ(db->Find(leaf_key, data), MBError::SUCCESS);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  data.data_len),
        leaf_value);
}

TEST_F(UpdateTest, BorrowedValuePointerLowerBoundLookup)
{
    ASSERT_EQ(db->Add("borrowed-a", "value-a"), MBError::SUCCESS);
    ASSERT_EQ(db->Add("borrowed-c", "value-c"), MBError::SUCCESS);

    MBData data(0, CONSTS::OPTION_RETURN_DATA_PTR);
    std::string bound_key;
    ASSERT_EQ(db->FindLowerBound("borrowed-b", data, &bound_key),
        MBError::SUCCESS);
    ASSERT_NE(data.data_ptr, nullptr);
    EXPECT_EQ(bound_key, "borrowed-a");
    EXPECT_EQ(data.data_ptr,
        db->GetDataPtrByOffset(data.data_offset + DB::GetDataHeaderSize()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.data_ptr),
                  data.data_len),
        "value-a");

    data.options = 0;
    EXPECT_EQ(db->FindLowerBound("borrowed-0", data, &bound_key),
        MBError::NOT_EXIST);
    EXPECT_EQ(data.data_ptr, nullptr);
    EXPECT_EQ(data.data_len, 0);
}

}
