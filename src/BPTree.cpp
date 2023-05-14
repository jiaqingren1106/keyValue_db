// Searching on a B+ tree in C++

#include "../include/BPTree.h"
#include <cmath>

using namespace std;

/* Public B-tree APIs for Storage to write, create, and search B-Tree: */

void BPTree::writeBtreeToFile(std::vector<key_t> all_keys, std::fstream& wf) {

  int key_size = all_keys.size();

  std::vector<int> node_num_level;
  std::vector<std::vector<file_node> > node_2D;

  
  int temp = key_size;
  while (temp>1) {
    int num_nodes = std::ceil((double)temp/(double)B_TREE_NODE_SIZE);
    node_num_level.insert(node_num_level.begin(), num_nodes);
    temp = num_nodes;
  }


  int pos_idx = 0, data_idx=0, leaf_child_offset_idx=0;
  
  for (int i=0;i<node_num_level.size();i++) {
    int num = node_num_level[i];
    std::vector<file_node> level;

    for (int j=0;j<num;j++) {

      file_node node;
      node.size = 0;
      node.first_child_offset = -1;
      (i==node_num_level.size()-1)? node.IS_LEAF = true : node.IS_LEAF = false;
      node.pos = pos_idx++;

      if (i==node_num_level.size()-1) {
        while (data_idx < key_size && node.size < B_TREE_NODE_SIZE) {
          (node.key)[node.size] = all_keys[data_idx];
          node.size++;
          data_idx++;
        }
        node.first_child_offset = leaf_child_offset_idx;
        leaf_child_offset_idx+=node.size;
      }

      level.push_back(node);
    }
    node_2D.push_back(level);
  }


  for (int i=node_2D.size()-2;i>=0;i--) {
    std::vector<file_node>& parent_level = node_2D[i];
    std::vector<file_node>& child_level = node_2D[i+1];
    int child_level_idx = 0;
    for (auto &parent_node : parent_level) {
      while (parent_node.size<B_TREE_NODE_SIZE && child_level_idx < child_level.size()) {
        file_node &cur_child = child_level[child_level_idx];
        if (parent_node.size==0) {
          parent_node.first_child_offset = cur_child.pos;
        }
        parent_node.key[parent_node.size] = cur_child.key[cur_child.size-1];
        parent_node.size++;
        child_level_idx ++;
      }
    }
  }

  for (int i = 0; i < node_2D.size(); i++) {
    for (int j = 0; j < node_2D[i].size(); j++) {

      wf.write((char *)&node_2D[i][j], sizeof(file_node));
    }
  }
}


/*
input: 
  char* file_data: contain all b-tree internal node
  int file_length: size of file_data in bytes
  key_t query_key
  bool is_scan: if true, if query_key not found, return index the key that is closest but larger than query_key
return:
  int index: index to kv_pair if found; if not found and is_scan is False, return -1
*/
int BPTree::searchBTree(uint8_t* file_data, int file_length, key_t query_key, bool is_scan) {


  file_node *root_node = (file_node *)(file_data);
  file_node* cur = root_node;

  while (!cur->IS_LEAF) {
    bool cur_found = false;
    for (int k = 0; k< cur->size; k++) {
      if (k==0 && query_key <= cur->key[k]) {
          cur = (file_node *)(file_data + (cur->first_child_offset + k) * sizeof(file_node) );
          cur_found = true;
          break;
      } else if (k>0 && cur->key[k-1] < query_key && query_key <= cur->key[k]) {
          cur = (file_node *)(file_data + (cur->first_child_offset + k) * sizeof(file_node) );
          cur_found = true;
          break;
      } 
    }
    if (!cur_found && query_key > cur->key[cur->size-1]) {
      cur = (file_node *)(file_data + (cur->first_child_offset + cur->size-1) * sizeof(file_node) );
    }
  }

  for (int i=0; i < cur->size; i++) {
    if (cur->key[i]==query_key){
      // cout<<"current position: "<<cur->pos << " data offset: "<< cur->first_child_offset + i <<"\n";
      return cur->first_child_offset + i;
    }
  }

  if (is_scan) {
    for (int i=0; i < cur->size; i++) {
      if (cur->key[i]>query_key){
        // cout<<"current position: "<<cur->pos << " data offset: "<< cur->first_child_offset + i<<"\n";
        return cur->first_child_offset + i;
      }
    }  
    // cout<<"current position: "<<cur->pos << " data offset: "<< cur->first_child_offset + cur->size<<"\n";
    return cur->first_child_offset + cur->size-1;
  }
  return -1;
}
