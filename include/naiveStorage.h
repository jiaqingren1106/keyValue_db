#pragma once

#include "type.h"

#include "sstable.h"
#include "extendibleHash.h"

#include "evictionAlgo.h"

#include "storageType.h"

#include <vector>
#include <string>


class NaiveStorage : public StorageType {
public:
    std::string db_dir;
    std::vector<sstable> sorted_files;
    int max_ver;
    ExtendibleHashTable bufferpool;
    EvictionAlgo* evictionAlgo;
    db_config_t config;
    
    void Init(char* db_name, db_config_t config);
    void Flush(kv_pair *data, int data_size);
    val_t Get(key_t key);
    std::vector<kv_pair> Scan(key_t key1, key_t key2);
};
