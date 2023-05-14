#include "../include/simpleLRU.h"
#include <iostream>

SimpleLRU::SimpleLRU(int cap) { 
    capacity = cap;
}

   
bool SimpleLRU::get(int key) {
    auto it = map.find(key);
    if (it == map.end()) {
        return false;
    }
    cache.splice(cache.end(), cache, it->second);
    return true;
}

void SimpleLRU::refer(int key) {
    if (get(key)) {
        return;
    }
    put(key);
}

int SimpleLRU::accessAndUpdate(int pageNum) {
    int x = -1;
	if (map.find(pageNum) == map.end()) {// if the value is not found in the list
		if (map.size() == capacity) {//if the cache size is equal to the capacity
			x = cache.back();
			cache.pop_back();//removing the last element from the queue
            map.erase(x); //erasing the key value pair from the hash table
		}     
	}
	else {
		cache.erase(map.find(pageNum)->second);//if present erasing it 
	}
	cache.push_front(pageNum);//adding the value to the front
	map[pageNum] = cache.begin();//storing the address in the hashtable
    return x;
}

void SimpleLRU::evict(int pageNum) {
    cache.erase(map.find(pageNum)->second);//if present erasing it
    map.erase(pageNum);
}


void SimpleLRU::display() {
    for (auto it=cache.begin(); it != cache.end(); it++) {
        std::cout << *it << " ";
    }
}

void SimpleLRU::put(int key) {
    if (cache.size() == capacity) {
        int first_key = cache.front();
        cache.pop_front();
        map.erase(first_key);   
        std::cout << "kicked out " << first_key << std::endl;
    }
    cache.push_back(key);
    map[key] = --cache.end();
}