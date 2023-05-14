#pragma once

#include "type.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

#include "bloom_filter.h"
#include "BPTree.h"

/*
SST file data structure:

| sst_metadata_t | contiguous kv_pair data | bloom filter | B-tree internal nodes |

*/

typedef struct sst_metadata {
    int num_entries;
    int bloom_filter_size;
} sst_metadata_t;

class sstable {
  private:
    void completeSST();
public:
  std::string filename;
  sst_metadata_t file_info;
  int data_size;
  int version;
  db_config_t config;

  sstable(std::string  filename, int ver, db_config_t config) ;

  static sstable create_sst(kv_pair *data, int data_size, const char* directory, int cur_version, db_config_t config);

  bool Get(key_t key, uint8_t* filter, uint8_t* b_tree_nodes, kv_pair* out_buf);

  void Scan(key_t key1, key_t key2, std::vector<kv_pair>& res);

  static sstable MergeSSTs(sstable new_sst, sstable old_sst, const char* out_filename, int max_lvl);

  void GetBloomFilter(uint8_t* filter);
  
  void GetBTree(uint8_t* internal_nodes);
 
};
