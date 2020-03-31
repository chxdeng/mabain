import com.swigi.mabainji.*;

public class MabainTest {
    static {
        System.loadLibrary("mabainJava");
    }

    public static void insertionTest(Mabain mdb, int count) {
        int added = 0;
        for (int i = 0; i < count; i++) {
            String key = "TEST_KEY_" + Integer.toString(i);
            String val = "TEST_VAL_" + Integer.toString(i);
            int rval = mdb.mbAdd(key, val);
            if (rval == 0) {
                added++;
            }
        }
        System.out.println("added " + Integer.toString(added) + " key-value pairs");
    }

    public static void queryTest(Mabain mdb, int count) {
        int found = 0;
        for (int i = 0; i < count; i++) {
            String key = "TEST_KEY_" + Integer.toString(i);
            mb_query_result result = new mb_query_result();
            int rval = mdb.mbFind(key, result);
            if (rval == 0) {
                found++;
            }
        }
        System.out.println("found " + Integer.toString(found) + " key-value pairs");
    }

    public static void gcTest(Mabain mdb) {
        int rval;
        rval = mdb.mbGC(32*1024*1023, 32*1024*1024);
        if (rval == 0) {
            System.out.println("successfully ran mabain gc");
        }
    }

    public static void deleteTest(Mabain mdb, int count) {
        int removed = 0;
        for (int i = 0; i < count; i++) {
            String key = "TEST_KEY_" + Integer.toString(i);
            int rval = mdb.mbDelete(key);
            if (rval == 0) {
                removed++;
            }
        }
        System.out.println("deleted " + Integer.toString(removed) + " key-value pairs");
    }

    public static void main(String[] args) {
        Mabain mdb = new Mabain("/var/tmp/mabain_test", true, true);
        if (!mdb.mbIsOpen()) {
            return;
        }
        int count = 100;
        insertionTest(mdb, count);
        queryTest(mdb, count);
        gcTest(mdb);
        deleteTest(mdb, count);
    }
}
