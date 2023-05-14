// A simple impl of memtable

#include <iostream>

#include "../include/simpleMem.h"

#include <utility>

// stores a key associated with a value
void SimpleMemtable::Put(key_t key, val_t value) {
    this->table[key] = value;
}
// retrieves a value associated with a given key
val_t SimpleMemtable::Get(key_t key) {
    if (this->table.find(key) != this->table.end()) {
        return this->table[key];
    } else {
        return NOT_FOUND;
    }
}

// retrieves all KV-pairs in a key range in key order (key1 < key2)
std::vector<kv_pair> SimpleMemtable::Scan(key_t key1, key_t key2) {
//   kv_pair *results = (kv_pair *)malloc(sizeof(kv_pair) * this->table.size());
  std::vector<kv_pair> results;
  if (this->table.size() <= 0) {
    return results;
  }
//   std::cout << this->table.size() << "\n";
  std::map<key_t, val_t>::iterator it;
  for (it = this->table.begin(); it != this->table.end(); it++) {
    // std::cout << "looking for content" << "\n";
    key_t k = it->first;
    val_t v = it->second;
    if (k >= key1 && k <= key2) {
        kv_pair pair;
        pair.key = k;
        pair.value = v;
        results.push_back(pair);
    }
  }
  return results;
}

int SimpleMemtable::GetNum() {
return this->table.size();
}
// empty all the data in memtable
void SimpleMemtable::Clear() { this->table.clear(); }
