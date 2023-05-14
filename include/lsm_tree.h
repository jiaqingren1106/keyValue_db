#pragma once

#include "type.h"

#include "sstable.h"
#include "extendibleHash.h"

#include "evictionAlgo.h"

#include "naiveStorage.h"

#include <vector>
#include <string>


class LSMTree : public NaiveStorage {
private:
    void Compaction();
public:
    int depth;
    
    void Init(char* db_name, db_config_t config);
    void Flush(kv_pair *data, int data_size);
    // val_t Get(key_t key);
    // std::vector<kv_pair> Scan(key_t key1, key_t key2);
};
