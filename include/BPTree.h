#pragma once

#include <climits>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "type.h"

struct file_node {
  int size;
  bool IS_LEAF;
  int first_child_offset;
  int pos;
  key_t key[B_TREE_NODE_SIZE];
};

// B tree
class BPTree {
  public:
    static void writeBtreeToFile(std::vector<key_t> all_keys, std::fstream& wf);
    static int searchBTree(uint8_t* file_data, int file_length, key_t query_key, bool is_scan);
};
