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
            unsigned char hash[SHA_DIGEST_LENGTH];
            EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
            if (mdctx == NULL) break;
            if (EVP_DigestInit_ex(mdctx, EVP_sha1(), NULL) != 1) { EVP_MD_CTX_free(mdctx); break; }
            if (EVP_DigestUpdate(mdctx, (unsigned char*)&key, 4) != 1) { EVP_MD_CTX_free(mdctx); break; }
            unsigned int hash_len;
            if (EVP_DigestFinal_ex(mdctx, hash, &hash_len) != 1) { EVP_MD_CTX_free(mdctx); break; }
            EVP_MD_CTX_free(mdctx);
            int i = 0;
            for (i = 0; i < SHA_DIGEST_LENGTH; i++) {
                sprintf(key_buff + (i * 2), "%02x", hash[i]);
            }
            key_buff[32] = 0;
        } break;
        case MABAIN_TEST_KEY_TYPE_SHA_256: {
            unsigned char hash[SHA256_DIGEST_LENGTH];
            EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
            if (mdctx == NULL) break;
            if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) { EVP_MD_CTX_free(mdctx); break; }
            if (EVP_DigestUpdate(mdctx, (unsigned char*)&key, 4) != 1) { EVP_MD_CTX_free(mdctx); break; }
            unsigned int hash_len;
            if (EVP_DigestFinal_ex(mdctx, hash, &hash_len) != 1) { EVP_MD_CTX_free(mdctx); break; }
            EVP_MD_CTX_free(mdctx);
            int i = 0;
            for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
                sprintf(key_buff + (i * 2), "%02x", hash[i]);
            }
            key_buff[64] = 0;
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
