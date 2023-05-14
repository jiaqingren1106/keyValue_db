#pragma once

#include "type.h"
#include "evictionAlgo.h"
#include <vector>

using namespace std;
class ClockEviction : public EvictionAlgo {
    public:
        ClockEviction(int capacity);
        int capacity;
        int clockHand;
        int load;
        vector<int> bitmap;
        vector<int> pagemap;
        // ExtendibleHashTable* pagehashmap;
        

        int accessAndUpdate(int pageNum);
        int evictAndReplace(int pageNum);
        void evict(int pageNum);
        void display();
};