#pragma once

#include <climits>

typedef int key_t;
typedef int val_t;

#define MIN_KEY INT_MIN
#define MAX_KEY INT_MAX

#define TOMBSTONE MIN_KEY // value that used as Tombstone
#define NOT_FOUND MAX_KEY

#define PAGE_SIZE 4096 // size of a page in bytes

#define B_TREE_NODE_SIZE 512 // number of entries in b-tree nodes s.t. the size is 4KB

// size of kv_pair should be aligned to PAGE_SIZE
typedef struct kv_pair {
    key_t key;
    val_t value;
} kv_pair;

enum eviction_algo_t {CLOCK, LRU};

enum sst_type_t {BINARY_SEARCH, B_TREE};

enum storage_type_t {LSM_TREE, NAIVE};

typedef struct config {
    int memtable_size;
    // buffer pool configs:
    eviction_algo_t eviction_algo;
    bool use_bufferpool;
    int bufferpool_capacity;
    // SST configs
    sst_type_t sst_type;
    // Bloom filter configs:
    int bits_per_entry = 0;
    // storage configs
    storage_type_t storage_type = NAIVE;

} db_config_t;
