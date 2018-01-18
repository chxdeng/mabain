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

#include <openssl/sha.h>

#define MABAIN_TEST_KEY_TYPE_INT        0
#define MABAIN_TEST_KEY_TYPE_SHA_128    1
#define MABAIN_TEST_KEY_TYPE_SHA_256    2
#define MABAIN_TEST_KEY_BUFF_SIZE       1024

namespace {

class TestKey
{
public:
    TestKey(int ktype) {
        key_type = ktype;
        key_buff[0] = '\0';
    }
    ~TestKey() {
    }
    const char* get_key(int key) {
        switch(key_type) {
            case MABAIN_TEST_KEY_TYPE_INT:
                snprintf(key_buff, MABAIN_TEST_KEY_BUFF_SIZE, "%d", key);
                break;
            case MABAIN_TEST_KEY_TYPE_SHA_128:
                {
                    unsigned char hash[SHA_DIGEST_LENGTH];
                    SHA_CTX sha1;
                    SHA1_Init(&sha1);
                    SHA1_Update(&sha1, (unsigned char*)&key, 4);
                    SHA1_Final(hash, &sha1);
                    int i = 0;
                    for(i = 0; i < SHA_DIGEST_LENGTH; i++)
                    {
                        sprintf(key_buff + (i * 2), "%02x", hash[i]);
                    }
                    key_buff[32] = 0;
                }
                break;
            case MABAIN_TEST_KEY_TYPE_SHA_256:
                {
                    unsigned char hash[SHA256_DIGEST_LENGTH];
                    SHA256_CTX sha256;
                    SHA256_Init(&sha256);
                    SHA256_Update(&sha256, (unsigned char*)&key, 4);
                    SHA256_Final(hash, &sha256);
                    int i = 0;
                    for(i = 0; i < SHA256_DIGEST_LENGTH; i++)
                    {
                        sprintf(key_buff + (i * 2), "%02x", hash[i]);
                    }
                    key_buff[64] = 0;
                }
                break;
            default:
                abort();
        }
        return (const char *) key_buff;
    }

private:
    int key_type;
    char key_buff[MABAIN_TEST_KEY_BUFF_SIZE];
};

}
