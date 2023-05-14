#include "../include/simpleBuf.h"

#include <algorithm>

#define PAGE_SIZE 4096

kv_pair* SimpleBufferPool::Get(key_t key){
    for (kv_pair* page : buffer) {
        for (int i = 0; i < (PAGE_SIZE / sizeof(kv_pair)); i++) {
            kv_pair pair = page[i];
            if (key == pair.key) {
                return page;
            }
        }
    }
}
void SimpleBufferPool::Put(kv_pair* page){
    if (std::find(buffer.begin(), buffer.end(), page) == buffer.end()){
    buffer.push_back(page);
    }
}
void SimpleBufferPool::Delete(kv_pair* page){
    buffer.erase(std::remove(buffer.begin(), buffer.end(), page), buffer.end());
}