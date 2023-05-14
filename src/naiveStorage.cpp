
#include "../include/type.h"
#include "../include/naiveStorage.h"

#include "../include/clockEviction.h"
#include "../include/simpleLRU.h"

#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <cstring>
#include <unistd.h>

#include <set>

namespace {
    // function used to sort SST files
    struct sort_sst {
        bool operator() (sstable& sst1, sstable& sst2) {
            // larger version number indicates more recent SST
            return (sst1.version > sst2.version);
        }
    };
}

void NaiveStorage::Init(char* db_name, db_config_t config){
    this->config = config;
    this->db_dir = db_name;
    this->max_ver = 0;

    if (this->config.eviction_algo == CLOCK) {
        this->evictionAlgo = new ClockEviction(this->config.bufferpool_capacity);
    } else {
        this->evictionAlgo = new SimpleLRU(this->config.bufferpool_capacity);
    }
    
    struct stat sb;
    if (stat(db_name, &sb) == 0){
        // directory exists
        struct dirent* entry = nullptr;
        DIR *dp = opendir(db_name);
        entry = readdir(dp); // entry = first entry
        std::string dir = db_name;
        while (entry) {
            std::string entry_name = entry->d_name;
            if (entry_name != "." && entry_name != "..") {
                int ver = std::stoi(entry_name);
                std::string fname = dir + "/" + entry_name;
                auto sst = sstable(fname, ver, this->config);
                this->sorted_files.push_back(sst);

                if (ver > this->max_ver) {
                    this->max_ver = ver;
                }
            }
            entry = readdir(dp);
        }
        closedir(dp);
    } else {
        mkdir(db_name, 0777);
    }
    // sorted from youngest to oldest
    std::sort(this->sorted_files.begin(), this->sorted_files.end(), sort_sst());
}

void NaiveStorage::Flush(kv_pair *data, int data_size){
    auto sst = sstable::create_sst(data, data_size, this->db_dir.c_str(), this->max_ver+1, this->config);
    this->sorted_files.insert(this->sorted_files.begin(), sst);
    this->max_ver++;
}


val_t NaiveStorage::Get(key_t key){
    
    bool key_found = false;
    kv_pair* sst_page = nullptr;

    if (this->config.use_bufferpool) 
    {
        /* First, find key in bufferpool */
        chain_node_t* pool_page = this->bufferpool.Get(key);
        if (pool_page == nullptr) {
            // Buffer pool miss! 
            // if not in pool, find key in SST
            // allocate a buffer in bufferpool
            pool_page = this->bufferpool.NewPage(PAGE_SIZE, KV_PAIR, -1);
            /* Find key in each SST */
            for (auto sst : this->sorted_files) {
                sst_metadata_t sst_info = sst.file_info;

                /* check if Bloom Filter in bufferpool */
                chain_node_t* filter_page = this->bufferpool.GetPage(sst.version, BLOOM_FILTER);
                if (filter_page == nullptr) {
                    // bloom filter not in bufferpool, fetch it from storage
                    filter_page = this->bufferpool.NewPage(sst_info.bloom_filter_size,
                                                BLOOM_FILTER, sst.version);
                    sst.GetBloomFilter(filter_page->page);
                }
                // buffer pool eviction algo
                int page_to_evict = evictionAlgo->accessAndUpdate(filter_page->page_num);
                if (page_to_evict != -1) {
                    this->bufferpool.Delete(page_to_evict);
                }

                /* check if B-Tree Internal Node in bufferpool */
                chain_node_t* b_tree_page = this->bufferpool.GetPage(sst.version, B_TREE_NODE);
                if (b_tree_page == nullptr) {
                    // b-tree internal nodes not in bufferpool, fetch it
                    int b_tree_size = sst.data_size - 
                                        (sizeof(sst_metadata_t)
                                        + sizeof(kv_pair) * sst_info.num_entries
                                        + sst_info.bloom_filter_size);
                    b_tree_page = this->bufferpool.NewPage(b_tree_size, B_TREE_NODE, sst.version);
                    sst.GetBTree(b_tree_page->page);
                }
                // buffer pool eviction algo
                page_to_evict = evictionAlgo->accessAndUpdate(b_tree_page->page_num);
                if (page_to_evict != -1) {
                    this->bufferpool.Delete(page_to_evict);
                }

                /* find the page that contains the key in storage*/
                if (sst.Get(key, filter_page->page, b_tree_page->page, (kv_pair*)pool_page->page)) {
                    key_found = true;
                    pool_page->src_file = sst.version;
                    break;
                }
            }
            if (key_found) {
                // put the page number into the eviction algorithm
                // evict pages if necessary
                int new_page_num = pool_page->page_num;
                int page_to_evict = evictionAlgo->accessAndUpdate(new_page_num);
                if (page_to_evict != -1) {
                    this->bufferpool.Delete(page_to_evict);
                }
                
            } else {
                // pool_page is empty since key is not found
                // de-allocate the page in bufferpool since key is not found
                this->bufferpool.Delete(pool_page->page_num);
            }
        } else {
            // bufferpool hit, no need to go into storage
            // Buffer pool hit!
            key_found = true;
            // update in eviction algorithm
            int hit_page_num = pool_page->page_num;
            evictionAlgo->accessAndUpdate(hit_page_num);
        }
        sst_page = (kv_pair*)pool_page->page;
    } 
    else 
    {
        uint8_t tmp[PAGE_SIZE];
        sst_page = (kv_pair*) tmp;
        /* No bufferpool, just search from storage */
        for (auto sst : this->sorted_files) {
            sst_metadata_t sst_info = sst.file_info;

            uint8_t* filter = (uint8_t*)malloc(sst_info.bloom_filter_size);
            
            int b_tree_size = sst.data_size - 
                                (sizeof(sst_metadata_t)
                                + sizeof(kv_pair) * sst_info.num_entries
                                + sst_info.bloom_filter_size);
            uint8_t* b_tree = (uint8_t*)malloc(b_tree_size);

            sst.GetBloomFilter(filter);
            sst.GetBTree(b_tree);

            if (sst.Get(key, filter, b_tree, sst_page)) {
                key_found = true;
            }
            free(filter);
            free(b_tree);
            if (key_found) {
                break;
            }
        }
    }

    // Once page is found, find the specific key in the page
    if (key_found) {
        // TODO: can change to binary search for optimization
        for (int i = 0; i < PAGE_SIZE / sizeof(kv_pair); i++) {
            if (sst_page[i].key == TOMBSTONE) { // reached the end
                break;
            } else if (sst_page[i].key == key) {
                return sst_page[i].value;
            }
        }
    }
    
    return TOMBSTONE;
}


std::vector<kv_pair> NaiveStorage::Scan(key_t key1, key_t key2){
    std::set<key_t> scan_keys;
    std::vector<kv_pair> res;
    // combine the scan result of each SST.
    for (int i=0;i<sorted_files.size();i++) {
        std::vector<kv_pair> sub_res;
        sorted_files[i].Scan(key1, key2, sub_res);
        for (auto pair : sub_res) {
            // since SSTs are sorted from youngest to oldest,
            // if a key is already encountered in previous SSTs,
            // we can discard the new key.
            if (!scan_keys.count(pair.key)) {
                scan_keys.insert(pair.key);
                res.push_back(pair);
            }
        }
    }
    return res;
}
