
#include "../include/type.h"
#include "../include/lsm_tree.h"

#include "../include/simpleLRU.h"
#include "../include/clockEviction.h"

#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <cstring>
#include <unistd.h>

#include <set>

#include <iostream>

namespace {
    // function used to sort SST files
    struct sort_sst {
        bool operator() (sstable& sst1, sstable& sst2) {
            // smaller version number indicates more recent SST
            return (sst1.version < sst2.version);
        }
    };
}

void LSMTree::Init(char* db_name, db_config_t config){
    NaiveStorage::Init(db_name, config);
    this->depth = this->max_ver + 1;
    // sorted from youngest to oldest
    std::sort(this->sorted_files.begin(), this->sorted_files.end(), sort_sst());
}

// Always merging the first two SSTs. SST with version -1 is the newest data, 
// the other one is the SST to be merged.
void LSMTree::Compaction() {
    auto new_sst = this->sorted_files.front(); // with version -1
    this->sorted_files.erase(this->sorted_files.begin());
    auto old_sst = this->sorted_files.front(); // with non-negative version
    int old_ver = old_sst.version;
    this->sorted_files.erase(this->sorted_files.begin());

    // merged to file with name -2.
    std::string tmp_fname = this->db_dir + "/" + "-2";
    sstable merged_sst = sstable::MergeSSTs(new_sst, old_sst, tmp_fname.c_str(), this->depth);
    // remove the two SST since they are already merged
    remove(new_sst.filename.c_str());
    remove(old_sst.filename.c_str());
    // rename the newly merged SST to -1 to indicate that it might need to be merged again
    std::string new_fname = this->db_dir + "/" + "-1";
    rename(merged_sst.filename.c_str(), new_fname.c_str());
    merged_sst.filename = new_fname;
    merged_sst.version = -1;
    
    // Evict dirty pages from buffer pool
    auto dirty_page_nums = this->bufferpool.DeleteDirtyPages(old_ver);
    // inform evict algo about the deleted pages
    for (auto num : dirty_page_nums) {
        evictionAlgo->evict(num);
    }

    int new_lvl = (old_sst.version + 1);
    if (this->sorted_files.size() > 0) {
        int cur_top_lvl = this->sorted_files[0].version;
        if (cur_top_lvl == new_lvl) {
            // need to continue merging since something exists in the current level
            this->sorted_files.insert(this->sorted_files.begin(), merged_sst);
            // recursive call to continue merging
            this->Compaction();
            return;
        }
    }
    // Otherwise, nothing exists in the current level, no need to continue merging
    new_fname = this->db_dir + "/" + std::to_string(new_lvl);
    rename(merged_sst.filename.c_str(), new_fname.c_str());
    merged_sst.filename = new_fname;
    merged_sst.version = new_lvl;
    this->sorted_files.insert(this->sorted_files.begin(), merged_sst);
    // update number of levels in LSM tree
    if ((new_lvl + 1) > this->depth) {
        this->depth = new_lvl + 1;
    }
}

void LSMTree::Flush(kv_pair *data, int data_size){
    if (this->sorted_files.size() > 0) {
        int cur_top_lvl = this->sorted_files[0].version;
        if (cur_top_lvl == 0) {
            // we have something in first level, needs to merge
            // the SST to be merged is labelled with version of -1
            auto sst = sstable::create_sst(data, data_size, this->db_dir.c_str(), -1, this->config);
            this->sorted_files.insert(this->sorted_files.begin(), sst);
            this->Compaction();
            return;
        }
    }
    // nothing in first level, no need to merge, just flush
    auto sst = sstable::create_sst(data, data_size, this->db_dir.c_str(), 0, this->config);
    this->sorted_files.insert(this->sorted_files.begin(), sst);
}

