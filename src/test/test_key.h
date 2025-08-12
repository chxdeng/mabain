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

#include <openssl/evp.h>
#include <openssl/sha.h>

#define MABAIN_TEST_KEY_TYPE_INT 0
#define MABAIN_TEST_KEY_TYPE_SHA_128 1
#define MABAIN_TEST_KEY_TYPE_SHA_256 2
#define MABAIN_TEST_KEY_BUFF_SIZE 1024

namespace {

class TestKey {
public:
    TestKey(int ktype)
    {
        key_type = ktype;
        key_buff[0] = '\0';
    }
    ~TestKey()
    {
    }
    const char* get_key(int key)
    {
        switch (key_type) {
        case MABAIN_TEST_KEY_TYPE_INT:
            snprintf(key_buff, MABAIN_TEST_KEY_BUFF_SIZE, "%d", key);
            break;
        case MABAIN_TEST_KEY_TYPE_SHA_128: {
            unsigned char hash[EVP_MAX_MD_SIZE];
            unsigned int hash_len;

            EVP_MD_CTX* ctx = EVP_MD_CTX_new();
            if (!ctx) {
                key_buff[0] = 0;
                break;
            }

            if (EVP_DigestInit_ex(ctx, EVP_sha1(), NULL) != 1 || EVP_DigestUpdate(ctx, (unsigned char*)&key, 4) != 1 || EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
                EVP_MD_CTX_free(ctx);
                key_buff[0] = 0;
                break;
            }

            EVP_MD_CTX_free(ctx);

            for (unsigned int i = 0; i < hash_len; i++) {
                sprintf(key_buff + (i * 2), "%02x", hash[i]);
            }
            key_buff[hash_len * 2] = 0;
        } break;
        case MABAIN_TEST_KEY_TYPE_SHA_256: {
            unsigned char hash[EVP_MAX_MD_SIZE];
            unsigned int hash_len;

            EVP_MD_CTX* ctx = EVP_MD_CTX_new();
            if (!ctx) {
                key_buff[0] = 0;
                break;
            }

            if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 || EVP_DigestUpdate(ctx, (unsigned char*)&key, 4) != 1 || EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
                EVP_MD_CTX_free(ctx);
                key_buff[0] = 0;
                break;
            }

            EVP_MD_CTX_free(ctx);

            for (unsigned int i = 0; i < hash_len; i++) {
                sprintf(key_buff + (i * 2), "%02x", hash[i]);
            }
            key_buff[hash_len * 2] = 0;
        } break;
        default:
            abort();
        }
        return (const char*)key_buff;
    }

private:
    int key_type;
    char key_buff[MABAIN_TEST_KEY_BUFF_SIZE];
};

}
