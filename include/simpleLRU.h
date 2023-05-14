#pragma once

#include "type.h"
#include "evictionAlgo.h"
#include <list>
#include <unordered_map>

using namespace std;
class SimpleLRU : public EvictionAlgo {
    public:
        SimpleLRU(int capacity);
        int capacity;
        list<int> cache;
        unordered_map<int, list<int>::iterator> map;

        bool get(int key);
        void put(int key);
        void refer(int key);

        int accessAndUpdate(int pageNum);
        void evict(int pageNum);
        
        void display();
};