#ifndef __MABAIN_C_INTERFACE_H__
#define __MABAIN_C_INTERFACE_H__

class mb_query_result {
public:
    char *data;
    int len;

    mb_query_result() {
        data = NULL;
        len = 0;
    }
    ~mb_query_result() {
        if(data != NULL) free(data);
    }
    char* get() {
        return data;
    }
};

#ifdef __cplusplus
extern "C" {
#endif

void* mbOpen(const char *db_dir, int writer_mode);
void  mbClose(void *db);
int   mbAdd(void *db, const char *key, int key_len, const char *data, int data_len);
int   mbRemove(void *db, const char *key, int key_len);
int   mbFind(void *db, const char *key, int len, mb_query_result *result);

#ifdef __cplusplus
}
#endif

#endif