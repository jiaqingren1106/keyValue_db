#include "../include/memtable.h"

/* Public API implementations for memtable */

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

/* Private helpers for memtable */

void Memtable::inOrderTraversal(AVLnode* root, key_t key1, key_t key2, std::vector<kv_pair>& result) {
    if (root != nullptr) {
        if (root->key >= key1 && root->key <= key2) {
            inOrderTraversal(root->left, key1, key2, result);
//             if (root->value != TOMBSTONE) {
                kv_pair pair;
                pair.key = root->key;
                pair.value = root->value;
                result.push_back(pair);
//             }
            inOrderTraversal(root->right, key1, key2, result);
        } else {
            if (root->key > key2) {
                inOrderTraversal(root->left, key1, key2, result);
            }
            if (root->key < key1) {
                inOrderTraversal(root->right, key1, key2, result);
            }
        }
    } else {
        return;
    }
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
        return NOT_FOUND;
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
    inOrderTraversal(cur_node, key1, key2, res);
//     for (int i=(int)key1;i<(int)key2 + 1;i++) {
//         val_t val = this->_Get(i, cur_node);
// 
//         if (val != TOMBSTONE) {
//             kv_pair pair;
//             pair.key = key1;
//             pair.value = val;
//             res.push_back(pair);
//         }
//     }
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

