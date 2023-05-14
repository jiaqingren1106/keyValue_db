
#include "../include/extendibleHash.h"


namespace {// functions that are private to this file:
        
    // Helper function to generate hash.
    hash_val_t hash_page(uint8_t* page, size_t data_size) {
        return XXHash32::hash(page, data_size, 443);
    }

    // Helper to merge two linked lists (i.e. chains)
    chain_node_t* merge_chains(chain_node_t* chain1, chain_node_t* chain2) {
        if (!chain1) {
            return chain2;
        }
        if (!chain2) {
            return chain1;
        }
        if (chain1 == chain2) {
            return chain1;
        }
        chain_node_t* prev_node = nullptr;
        chain_node_t* cur_node = chain1;
        while (cur_node) {
            prev_node = cur_node;
            cur_node = cur_node->next;
        }
        prev_node->next = chain2;
        chain2->prev = prev_node;
        return chain1;
    }
}

// Construct the initial extendible hash table
ExtendibleHashTable::ExtendibleHashTable(){
    prefix_len = INIT_PREFIX_LEN;
    dir_size = 1 << prefix_len; // equivalent to 2^(prefix_len)
    directories = (chain_node_t**) malloc(sizeof(chain_node_t*) * dir_size);
    for (int i = 0; i < dir_size; i++) {
        directories[i] = nullptr;
    }
    cur_num_pages = 0;
    cur_size = 0;
    page_id = 0;
}

// De-construct the extendible hash table
ExtendibleHashTable::~ExtendibleHashTable(){
    for (size_t i = 0; i < dir_size; i++) {
        chain_node_t* cur_node = directories[i];
        while (cur_node) {
            chain_node_t* next_node = cur_node->next;
            free(cur_node);
            cur_node = next_node;
        }
    }
    free(directories);
}

// Helper to insert the given node into the correct chain.
void ExtendibleHashTable::AddNode(chain_node_t* new_node){
    auto hashing = hash_page(new_node->page, new_node->size);
    hash_val_t prefix = GetPrefix(hashing);
    // get the start of the hash bucket
    chain_node_t* cur_node = directories[prefix];
    if (cur_node) {
        // inserted after the first node in chain
        chain_node_t* next_node = cur_node->next;
        cur_node->next = new_node;
        new_node->prev = cur_node;
        new_node->next = next_node;
        if (next_node) {
            next_node->prev = new_node;
        }
    } else {
        // or be the first node in chain
        directories[prefix] = new_node;
    }
}

// Helper to unlink the given node from the chain
void ExtendibleHashTable::UnLinkNode(chain_node_t* node){
    chain_node_t* prev_node = node->prev;
    chain_node_t* next_node = node->next;
    if (prev_node){
        prev_node->next = next_node;
        if (next_node) {
            next_node->prev = prev_node;
        }
    } else {
        // if it is the first node in the chain to unlink
        for (size_t pref = 0; pref < dir_size; pref++) {
            // unlink every prefix that points to this node
            if (directories[pref] == node) {
                directories[pref] = next_node;
                if (next_node) {
                    next_node->prev = nullptr;
                }
            }
        }
    }
}

// Expand the directory by a factor of 2. NOTE: no rehashing is performed in this function.
void ExtendibleHashTable::ExpandDir() {
    size_t new_prefix_len = prefix_len + 1;
    size_t new_dir_size = dir_size * 2;
    chain_node_t** new_dir = (chain_node_t**) malloc(sizeof(chain_node_t*) * new_dir_size);
    for (size_t idx = 0; idx < new_dir_size; idx++) {
        auto old_idx = idx >> 1;
        new_dir[idx] = directories[old_idx];
    }
    free(directories);
    directories = new_dir;
    prefix_len = new_prefix_len;
    dir_size = new_dir_size;
    // printf("After expand, size: %d\n", dir_size);
}

// Given a prefix from the directory after expansion, pick out the pages that has hashing value
// same prefix as the given prefix, and assign them to the new bucket.
void ExtendibleHashTable::ReHashing(hash_val_t prefix) {
    chain_node_t* cur_node = directories[prefix];
    directories[prefix] = nullptr;

    while (cur_node) {
        chain_node_t* next_node = cur_node->next;
        hash_val_t hashing = hash_page(cur_node->page, cur_node->size);
        if (GetPrefix(hashing) == prefix) {
            UnLinkNode(cur_node);
            cur_node->prev = nullptr;
            cur_node->next = nullptr;
            AddNode(cur_node);
        }
        cur_node = next_node;
    }
}

// Shrink the directory by a factor of 2. Merge chains if necessary.
void ExtendibleHashTable::ShrinkDir() {
    if (prefix_len <= INIT_PREFIX_LEN) {
        return;
    }
    size_t new_prefix_len = prefix_len - 1;
    size_t new_dir_size = dir_size / 2;
    chain_node_t** new_dir = (chain_node_t**) malloc(sizeof(chain_node_t*) * new_dir_size);
    for (size_t idx = 0; idx < new_dir_size; idx++) {
        auto old_idx_1 = idx << 1;
        auto old_idx_2 = old_idx_1 + 1;
        new_dir[idx] = merge_chains(directories[old_idx_1], directories[old_idx_2]);
    }
    free(directories);
    directories = new_dir;
    prefix_len = new_prefix_len;
    dir_size = new_dir_size;
    // printf("After shrink, size: %d\n", dir_size);
}

// Get page given a key.
chain_node_t* ExtendibleHashTable::Get(key_t key){
    for (hash_val_t prefix = 0; prefix < dir_size; prefix++) {
        chain_node_t* cur_node = directories[prefix];
        while (cur_node) {
            if (cur_node->data_type == KV_PAIR) {
                kv_pair* page = (kv_pair*)cur_node->page;
                // TODO: can optimize the following to be a binary search:
                for (unsigned int i = 0; i < cur_node->size/sizeof(kv_pair); i++) {
                    if (page[i].key == TOMBSTONE) {
                        break;
                    }
                    if (key == page[i].key) {
                        return cur_node;
                    }
                }
            }
            cur_node = cur_node->next;
        }
    }
    return nullptr;
}

chain_node_t* ExtendibleHashTable::GetPage(int src_file, buffer_data_t data_type) {
    for (hash_val_t prefix = 0; prefix < dir_size; prefix++) {
        chain_node_t* cur_node = directories[prefix];
        while (cur_node) {
            if (cur_node->data_type == data_type
                && cur_node->src_file == src_file) {
                uint8_t* page = (uint8_t*)cur_node->page;
                return cur_node;
            }
            cur_node = cur_node->next;
        }
    }
    return nullptr;       
}

chain_node_t* ExtendibleHashTable::NewPage(size_t data_size, buffer_data_t data_type, int file_ver) {
    auto new_node = (chain_node_t*) malloc(sizeof(chain_node_t));
    new_node->page_num = this->page_id;
    new_node->src_file = -1;
    new_node->prev = nullptr;
    new_node->next = nullptr;
    this->page_id++;
    
    new_node->page = (uint8_t*)malloc(data_size);
    new_node->size = data_size;
    new_node->data_type = data_type;
    new_node->src_file = file_ver;
    AddNode(new_node);
    this->cur_num_pages++;
    this->cur_size += data_size;

    // TODO: check whether this is the optimal condition to expand directory
    if (this->cur_num_pages >= this->dir_size) {
        ExpandDir();
        for (size_t pref = 0; pref < dir_size; pref++) {
            ReHashing(pref);
        }
    }
    return new_node;
}

// Delete a page from the hash table
void ExtendibleHashTable::Delete(int page_num){
    chain_node_t* cur_node = nullptr;
    for (hash_val_t prefix = 0; prefix < dir_size; prefix++) {
        cur_node = directories[prefix];
        while (cur_node) {
            if (cur_node->page_num == page_num){
                break;
            }
            cur_node = cur_node->next;
        }
    }
    
    if (cur_node) {
        UnLinkNode(cur_node);
        free(cur_node->page);
        cur_num_pages--;
        this->cur_size -= cur_node->size;
        free(cur_node);
        // TODO: check whether this is the optimal condition to shrink directory
        if (this->cur_num_pages <= (this->dir_size/2)) {
            ShrinkDir();
        }
    }
}

std::vector<int> ExtendibleHashTable::DeleteDirtyPages(int dirty_file) {
    // std::vector<int> removed_page_nums = {};
    std::vector<int> removed_page_nums;

    
    chain_node_t* cur_node = nullptr;
    for (hash_val_t prefix = 0; prefix < dir_size; prefix++) {
        cur_node = directories[prefix];
        while (cur_node) {
            chain_node_t* next_node = cur_node->next;
            if (cur_node->src_file == dirty_file) {
                // record what page has been deleted, return for eviction algorithm
                removed_page_nums.push_back(cur_node->page_num);
                // delete the dirty page
                UnLinkNode(cur_node);
                free(cur_node->page);
                cur_num_pages--;
                this->cur_size -= cur_node->size;
                free(cur_node);
            }
            cur_node = next_node;
        }
    }
    
    if (this->cur_num_pages <= (this->dir_size/2)) {
        ShrinkDir();
    }
    return removed_page_nums;
}


