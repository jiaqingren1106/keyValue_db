
#include "include/database.h"

#include <cstdio>
#include <iostream>
#include <stdlib.h>
#include <assert.h>

#include <set>

int main (int argc, char * argv[])
{
    Database db = Database();
    db_config_t config = {   
        .memtable_size = 100,
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = 100,
        .sst_type = B_TREE,
        .bits_per_entry = 5,
        .storage_type = LSM_TREE
    };
    
    
    srand(443);
    int db_size = 4096;
    
    db.Open("db_test", config);
    
    for (int i = 0; i < db_size; i++) {
        db.Put(i, i);
    }
    
    std::vector<key_t> query_keys;
    for (int _ = 0; _ < 100; _++) {
        key_t rand_k = rand() % db_size;
        query_keys.push_back(rand_k);
    }
    
    std::vector<key_t> nonexist_keys;
    for (int _ = 0; _ < 20; _++) {
        int del_idx = rand() % query_keys.size();
        key_t del_k = query_keys[del_idx];
        db.Delete(del_k);
        nonexist_keys.push_back(del_k);
    }
    std::sort(nonexist_keys.begin(), nonexist_keys.end());
    
    for (auto k : nonexist_keys) {
        std::cout << k << ",";
    }
    std::cout << "\n";
    std::cout << "Keys above are deleted" << std::endl;

    for (auto rand_k : query_keys) {
        val_t v = db.Get(rand_k);
        std::cout << "Get " << rand_k << std::endl;
        if (std::find(nonexist_keys.begin(), nonexist_keys.end(), rand_k) != nonexist_keys.end()) {
            std::cout << "Del key" <<std::endl;
            assert(v == TOMBSTONE);
        } else {
            std::cout << v << std::endl;
            assert(rand_k == v);
        }
    }
    
    int start = rand() % db_size;
    int range = rand() % 100;
    
    std::vector<kv_pair> scan_result = db.Scan(start,start+range);
    
    for (auto &pair : scan_result) {
        if (std::find(nonexist_keys.begin(), nonexist_keys.end(), pair.key) != nonexist_keys.end()
            || pair.key < 0 || pair.key >= db_size) {
            assert(false);
        } else {
            assert(pair.key == pair.value);
        }
    }

    db.Close();
}
