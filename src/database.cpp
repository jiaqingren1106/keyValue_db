// Database API implementations

#include "../include/database.h"

#include "../include/lsm_tree.h"
#include "../include/naiveStorage.h"

#include <iostream>
#include <string>
#include <algorithm>

#include <set>

//TODO: check google doc for instructions

void Database::FlushBufToStorage() {
    std::vector<kv_pair> buffer_data = this->memtable.Scan(MIN_KEY, MAX_KEY);
    int total_size = buffer_data.size();
    if (total_size > 0) {
//         printf("Flush data of size %d before closing\n", total_size);
        this->storage->Flush(buffer_data.data(), total_size*sizeof(kv_pair));
    }
}

 // opens your database and prepares it to run
void Database::Open(char* db_name, db_config_t config){
    this->config = config;
    this->database_name = db_name;
    // this->memtable = SimpleMemtable();
    if (config.storage_type == LSM_TREE) {
        this->storage = new LSMTree();
    } else {
        this->storage = new NaiveStorage();
    }
    this->storage->Init(db_name, config);
}

// stores a key associated with a value
void Database::Put(key_t key, val_t value){
    if (key == TOMBSTONE || value == NOT_FOUND) {
        perror("Invalid key or value\n");
        return;
    }
    this->memtable.Put(key, value);
    // flush when memtable is full
    if (this->memtable.GetNum() >= this->config.memtable_size) {
        this->FlushBufToStorage();
        this->memtable.Clear();
    } 
}

// retrieves a value associated with a given key
val_t Database::Get(key_t key){
    val_t value = this->memtable.Get(key);
    if (value == NOT_FOUND) {
        // key not in memtable, find it in storage
        return this->storage->Get(key);
    } else {
        return value;
    }
}

// retrieves all KV-pairs in a key range in key order (key1 < key2)
std::vector<kv_pair> Database::Scan(key_t key1, key_t key2){
    std::vector<kv_pair> buffer_data = this->memtable.Scan(key1, key2);
    // scan SSTs
    std::vector<kv_pair> storage_data = this->storage->Scan(key1, key2);
    
    // combine results from memtable and SSTs
    std::set<key_t> scan_keys;
    std::vector<kv_pair> result;
    for (auto buf_pair : buffer_data) {
        scan_keys.insert(buf_pair.key);
        result.push_back(buf_pair);
    }
    for (auto storage_pair : storage_data) {
        if (!scan_keys.count(storage_pair.key)) {
            // since memtable contains the most recent data,
            // if a key is already encountered in buffer,
            // we can discard the key from storage.
            result.push_back(storage_pair);
        }
    }
    
    struct scan_sort {
        bool operator() (kv_pair& pair1, kv_pair& pair2) {
            // larger version number indicates more recent SST
            return (pair1.key < pair2.key);
        }
    };
    std::sort(result.begin(), result.end(), scan_sort());
    
    std::vector<kv_pair> final_result;
    for (auto pair : result) {
        if (pair.value != TOMBSTONE) {
            final_result.push_back(pair);
        }
    }
    
    return final_result;
}


void Database::Delete(key_t key) {
    this->Put(key, TOMBSTONE);
}

// closes your database
void Database::Close(){
    this->FlushBufToStorage();
    delete this->storage;
    return;
}
