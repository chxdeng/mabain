package mabain_test

import (
	"fmt"
        "mabain"
        "strconv"
	"testing"
)

const (
	MABAIN_PATH = "/var/tmp/mabain_test"
	COUNT = 100
)

func TestInsertion(t *testing.T) {
	db := mabain.MbOpen(MABAIN_PATH, 1)
	for i := 0; i < COUNT; i++ {
		key := "TEST_KEY_" + strconv.Itoa(i)
		val := "TEST_VAL_" + strconv.Itoa(i)
		mabain.MbAdd(db, key, len(key), val, len(val))
	}
	mabain.MbClose(db)
}

func TestQuery(t *testing.T) {
	db := mabain.MbOpen(MABAIN_PATH, 1)
	found := 0

	result := mabain.NewMb_query_result()
	for i := 0; i < COUNT; i++ {
		key := "TEST_KEY_" + strconv.Itoa(i)
		rval := mabain.MbFind(db, key, len(key), result)
		if rval == 0 {
			fmt.Println(result.GetData())
			found++
		}
	}
	mabain.MbClose(db)
	fmt.Printf("Found %d KV paris", found)
}

func TestRemove(t *testing.T) {
	db := mabain.MbOpen(MABAIN_PATH, 1)
	for i := 0; i < COUNT; i++ {
		key := "TEST_KEY_" + strconv.Itoa(i)
		rval := mabain.MbRemove(db, key, len(key))
		if rval != 0 {
			fmt.Printf("remove test failed for key %s with rval %d", key, rval)
		}
	}
	mabain.MbClose(db)
}
