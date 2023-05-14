// Contains APIs of memtable to user

#pragma once

#include "type.h"

#include <cstddef>
#include <cstdio>

#include <algorithm>
#include <iostream>
#include <vector>


class AVLnode {
public:
  key_t key;
  val_t value;

  AVLnode *left = nullptr;
  AVLnode *right = nullptr;

  int height_self = 1;

  AVLnode(key_t key, val_t value) {
    this->key = key;
    this->value = value;
  }
};

class Memtable {
    public:
        // stores a key associated with a value
         void Put(key_t key, val_t value);
        // retrieves a value associated with a given key
         val_t Get(key_t key);
        // retrieves all KV-pairs in a key range in key order (key1 < key2)
         std::vector<kv_pair> Scan(key_t key1, key_t key2);

         int GetNum();

        // Clear all the data in the memtable
         void Clear();

private:
    int node_size = 0;
    AVLnode *root = nullptr;

    static void inOrderTraversal(AVLnode* root, key_t key1, key_t key2, std::vector<kv_pair>& result);

    void Free(AVLnode *cur_node);
    int compute_height(AVLnode *node);
    int compute_balance(AVLnode *node);
    AVLnode* left_rotate(AVLnode *node);
    AVLnode* right_rotate(AVLnode *node);

    AVLnode* _Put(key_t key, val_t value, AVLnode *cur_node);
    val_t _Get(key_t key, AVLnode *cur_node);
    std::vector<kv_pair> _Scan(key_t key1, key_t key2, AVLnode *cur_node);
    void _Clear(AVLnode *cur_node);
};
