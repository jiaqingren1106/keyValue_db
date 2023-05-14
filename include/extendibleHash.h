#pragma once

#include "type.h"

#include <stdlib.h>
#include <vector>
#include "xxhash32.h"

#include <stdio.h>

#define INIT_PREFIX_LEN 2

typedef uint32_t hash_val_t;

enum buffer_data_t {KV_PAIR, BLOOM_FILTER, B_TREE_NODE};

// Double Linked List node
typedef struct chain_node {
    int page_num;
    int src_file;
    uint8_t* page;
    size_t size;
    buffer_data_t data_type;
    struct chain_node* prev;
    struct chain_node* next;
} chain_node_t;

class ExtendibleHashTable {
    public:
        ExtendibleHashTable();
        ~ExtendibleHashTable();
        chain_node_t* Get(key_t key);
        chain_node_t* NewPage(size_t data_size, buffer_data_t data_type, int file_ver);
        void Delete(int page_num);
        std::vector<int> DeleteDirtyPages(int dirty_file);
        int GetCurSize() {
            return cur_size;
        }

        chain_node_t* GetPage(int src_file, buffer_data_t data_type);
    private:
        chain_node_t** directories;
        size_t prefix_len;
        size_t dir_size;
        size_t cur_size;
        size_t cur_num_pages;
        int page_id;
        
        // Return the prefix of the given length of the given hashing value
        hash_val_t GetPrefixOfLen(hash_val_t hashing, size_t length) {
            hash_val_t mask = (~0) << length; // e.g. 11110000, if hash_val_t is 8 bit and prefix_len is 4
            hash_val_t pref = mask & hashing;
            int avail_prefix = sizeof(hash_val_t)*8 - length;
            pref = pref >> avail_prefix;
            return pref;
        }
        
        hash_val_t GetPrefix(hash_val_t hashing) {
            return GetPrefixOfLen(hashing, prefix_len);
        }
        
        void AddNode(chain_node_t* node);
        void UnLinkNode(chain_node_t* node);
        
        void ExpandDir();
        void ReHashing(hash_val_t prefix);
        void ShrinkDir();
        
};
