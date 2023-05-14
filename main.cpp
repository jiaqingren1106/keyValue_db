// #include "include/database.h"
#pragma once
#include <cstddef>
#include <cstdio>

#include "include/type.h"
#include <algorithm>
#include <iostream>
#include <queue>
#include <vector>
// Contains APIs to memtable that are private to user



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
    static void inOrderTraversal(AVLnode* root, int depth);
    AVLnode *root = nullptr;

    void Put(key_t key, val_t value);
    val_t Get(key_t key);
    std::vector<kv_pair> Scan(key_t key1, key_t key2);
    int GetNum();
    void Clear();

private:
    int node_size = 0;

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

void Memtable::Put(key_t key, val_t value) {
    this->root = _Put(key, value, this->root);
}
val_t Memtable::Get(key_t key) {
    return _Get(key, this->root);
}
std::vector<kv_pair> Memtable::Scan(key_t key1, key_t key2) {
    return _Scan(key1, key2, this->root);
}
void Memtable::Clear() {
    _Clear(this->root);
    this->node_size = 0;
}
int Memtable::GetNum(){
    return this->node_size;
} 

void Memtable::inOrderTraversal(AVLnode* root, int depth) {
    std::cout<<"==========inorder depth: "<< depth <<"\n";
    if (root != nullptr) {
        for (int i=0; i<depth;i++) {
            std::cout << " ";
        }
        std::cout << "KEY: "<< root->key<< " height: "<< root->height_self << "\n";

        inOrderTraversal(root->left, depth+1);
        inOrderTraversal(root->right, depth+1);
    } else {
        std::cout<<"empty tree\n";
    }
    std::cout<<"=====inorder end\n\n";
}

AVLnode* Memtable::left_rotate(AVLnode *node){
    // std::cout << "\n!!!!!!!!!!!enter inorder traversal before function\n";
    // inOrderTraversal(node, 0);
    // std::cout << "!!!!!!!!!!!exit inorder traversal before function\n";

    AVLnode* node1 = node;
    AVLnode* node2 = node->right;
    node->right = nullptr;

    AVLnode* extra = node2->left;

    node2->left = node1;
    node1->right = extra;
    node->height_self = this->compute_height(node);

    // std::cout << "\n!!!!!!!!!!!enter inorder traversal after function\n";
    // inOrderTraversal(node2, 0);
    // std::cout << "!!!!!!!!!!!exit inorder traversal after function\n";

    return node2;
}

AVLnode* Memtable::right_rotate(AVLnode *node){
    AVLnode* node1 = node;
    AVLnode* node2 = node->left;
    node->left = nullptr;

    AVLnode* extra = node2->right;
    node2->right = node1;
    node1->left = extra;

    node->height_self = this->compute_height(node);

    return node2;
}


int Memtable::compute_height(AVLnode *node){
    if (node->left && node->right) {
        return std::max(node->right->height_self, node->left->height_self) + 1;
    } else if (node->left) {
        return node->left->height_self+1;
    } else if (node->right){
        return node->right->height_self+1;
    } else {
        return 1;
    }
}
int Memtable::compute_balance(AVLnode *node){
    if (node->left&&node->right) {
        return node->right->height_self - node->left->height_self;
    } else if (node->left) {
        return 0 - node->left->height_self;
    } else if (node->right) {
        return node->right->height_self;
    } else {
        return 0;
    }
}


AVLnode* Memtable::_Put(key_t key, val_t value, AVLnode *cur_node) {
  if (!cur_node) {
    cur_node = new AVLnode(key, value);
    node_size++;
    return cur_node;
  }

  if (key == cur_node->key) {
    cur_node->value = value;
    return cur_node;
  } else if (key < cur_node->key) {
    if (cur_node->left) {
        cur_node->left = this->_Put(key, value, cur_node->left);
    } else {
        cur_node->left = new AVLnode(key, value);
        node_size++;
    }
  } else {
    if (cur_node->right) {
      cur_node->right = this->_Put(key, value, cur_node->right);
    } else {
      cur_node->right = new AVLnode(key, value);
      node_size++;
    }
  }

    cur_node->height_self = this->compute_height(cur_node);
    int factor = this->compute_balance(cur_node);

    // right heavy
    if (factor > 1) {
        if (key > cur_node->right->key) {
            return this->left_rotate(cur_node);
        }         
        else{
            cur_node->right = this->right_rotate(cur_node->right);
            return this->left_rotate(cur_node);
        }
    } 
    
    // left heavy
    else if (factor < -1) {
        if (key < cur_node->left->key){  
            return this->right_rotate(cur_node);
        }
        else {   
            cur_node->left = this->left_rotate(cur_node->left);
            return this->right_rotate(cur_node);
        }
    }
    return cur_node;
}


val_t Memtable::_Get(key_t key, AVLnode *cur_node) {
    if (!cur_node) {
        // std::cout <<"invisibel key: "<< key <<"\n";
        return TOMBSTONE;
    }
    if (cur_node->key==key) {
        return cur_node->value;
    }
    if (key < cur_node->key) {
        return this->_Get(key, cur_node->left);
    }
    return this->_Get(key, cur_node->right);
}

std::vector<kv_pair> Memtable::_Scan(key_t key1, key_t key2, AVLnode *cur_node) {
    std::vector<kv_pair> res;
    for (int i=(int)key1;i<(int)key2 + 1;i++) {
        val_t val = this->_Get(i, cur_node);

        if (val != TOMBSTONE) {
            kv_pair pair;
            pair.key = key1;
            pair.value = val;
            res.push_back(pair);
        }
    }
    return res;
}

void Memtable::Free(AVLnode*cur_node) {
    if (!cur_node) {
        return;
    }
    if (!cur_node->left && !cur_node->right) {
        delete cur_node;
        return;
    }
    this->Free(cur_node->left);
    this->Free(cur_node->right);

    delete cur_node;
    return;
}
void Memtable::_Clear(AVLnode*cur_node) {
    this->Free(cur_node);
    this->root = nullptr;
}






int main (int argc, char * argv[])
{
    Memtable boo;


    // boo.root = boo.Put((key_t)1, (val_t)0, boo.root);
    //     std::cout << boo.Get(1, boo.root);

    // boo.root = boo.Put((key_t)1, (val_t)2, boo.root);
    // std::cout << boo.Get(1, boo.root);

    // boo.root = boo.Put((key_t)1, (val_t)1, boo.root);

    // std::cout << boo.Get(1, boo.root);




    std::vector<int> input_array{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int> result;

    std::srand(std::time(0));

    for (int i = 0; i < 10; ++i) {
        int random_index = std::rand() % input_array.size();
        result.push_back(input_array[random_index]);
    }

    std::cout << "Randomly inserted integers:" << std::endl;
    for (const auto &elem : result) {
        std::cout << elem << " ";
    }
    std::cout << std::endl;


    for (int i=0;i<result.size();i++ ) {
        boo.Put((key_t)result[i], (val_t)result[i]);
    }
    for (int i=0;i<result.size();i++ ) {
        std::cout << boo.Get((key_t)i)  <<"\n";
    }

std::cout << "in scaam !!!!!!!!\n";
    std::vector<kv_pair> res = boo.Scan(0, 10);
    for (int i=0;i<res.size();i++) {
        std::cout << (res[i].value) << "\n";
    }
    std::cout << "in scaam !!!!!!!! " << boo.GetNum() << "\n";

    

    boo.Clear();
    if (!boo.root) {
        std::cout << "fredd!!!\n";
    }
    std::cout << "in clear !!!!!!!! " << boo.GetNum() << "\n";

    return 0;
}