// A simple impl of memtable
#pragma once

#include "../include/memtable.h"

#include <map>
#include <vector>

class SimpleMemtable : public Memtable {
    private:
        std::map<key_t, val_t> table;
    public:
        // stores a key associated with a value
         void Put(key_t key, val_t value);
        // retrieves a value associated with a given key
         val_t Get(key_t key);
        // retrieves all KV-pairs in a key range in key order (key1 < key2)
         std::vector<kv_pair> Scan(key_t key1, key_t key2);

         int GetNum();

        // Clear all the data in the memtable
         void Clear();
};
