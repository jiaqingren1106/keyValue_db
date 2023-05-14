// Contains every database APIs that are public to the users
#pragma once

#include "simpleMem.h"
// #include "memtable.h"

#include "storageType.h"

#include <vector>

class Database {
private:
    void FlushBufToStorage();
    db_config_t config;
public:

        /* Attributes */
        SimpleMemtable memtable;
//         Memtable memtable;
        char* database_name;
        StorageType* storage;

    /* Public APIs to users*/
        // opens your database and prepares it to run
        void Open(char* db_name, db_config_t config);
        // stores a key associated with a value
        void Put(key_t key, val_t value);
        // retrieves a value associated with a given key
        val_t Get(key_t key);
        // retrieves all KV-pairs in a key range in key order (key1 < key2)
        std::vector<kv_pair> Scan(key_t key1, key_t key2);
        // delete
        void Delete(key_t key);
        // closes your database
        void Close();
};
