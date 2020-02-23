#!/usr/bin/python

import mabain
import os

def insertion_test(db_dir, count):
    db = mabain.mb_open(db_dir, 1);
    for i in range(count):
        key = "TEST_KEY_" + str(i);
        val = "TEST_VAL_" + str(i);
        mabain.mb_add(db, key, len(key), val, len(val));
    mabain.mb_close(db)

def query_test(db_dir, count):
    db = mabain.mb_open(db_dir, 1)
    result = mabain.mb_query_result()
    found = 0
    for i in range(count):
        key = "TEST_KEY_" + str(i);
        rval = mabain.mb_find(db, key, len(key), result)
        if rval == 0:
            print(result.get())
            found += 1
    mabain.mb_close(db)
    print("found " + str(found) + " KV pairs")

db_dir = "/var/tmp/mabain_test"
os.system("mkdir -p " + db_dir)
os.system("rm -rf " + db_dir + "/*")
insertion_test(db_dir, 100)
query_test(db_dir, 100)
