#include "../include/clockEviction.h"
#include <iostream>
#include <algorithm>

ClockEviction::ClockEviction(int cap) { 
    // for clock algo
    capacity = cap;
    clockHand = 0;
    load = 0;
    for (int i = 0; i < cap; i++) {
        bitmap.push_back(0);
        pagemap.push_back(-1);
    }
}

// return evicted page number, if no eviction, return -1   
int ClockEviction::accessAndUpdate(int pageNum) {
    auto itr=std::find(pagemap.begin(), pagemap.end(), pageNum);
    if (itr != pagemap.end()) {
        int index = std::distance(pagemap.begin(), itr);
        bitmap[index] = 1;
    }
    else {
        auto fill_itr=std::find(pagemap.begin(), pagemap.end(), -1);
        if (fill_itr != pagemap.end()) {
            int index = std::distance(pagemap.begin(), fill_itr);
            pagemap[index] = pageNum;
            bitmap[index] = 1;
        }
        else {
            return evictAndReplace(pageNum); 
        }
    }
    return -1;
  
}

// return evicted page number
int ClockEviction::evictAndReplace(int pageNum) {
    while (bitmap[clockHand] != 0) {
        bitmap[clockHand] = 0;
        if (clockHand == capacity-1) { // cycle
            clockHand = 0;
        }
        else {
            clockHand++;
        }
    }
    // pagehashmap->Delete((kv_pair*)pagemap[clockHand]);
    // pagehashmap->Put((kv_pair*)pageNum);
    int old_page_num = pagemap[clockHand];
    pagemap[clockHand] = pageNum;
    bitmap[clockHand] = 1;
    return old_page_num;
}

void ClockEviction::evict(int pageNum) {
    auto itr=std::find(pagemap.begin(), pagemap.end(), pageNum);
    if (itr != pagemap.end()) {
        int index = std::distance(pagemap.begin(), itr);
        pagemap[index] = -1;
        bitmap[index] = 0;
    }
    else {
        std::cout << "error: could not find pagenum" <<endl;
    }
}

void ClockEviction::display() {
    std::cout << "bitmap:" <<endl;
    for (auto i : bitmap) {
        std::cout << i << " ";
    }
    std::cout << endl;
    std::cout << "pagemap:" <<endl;
    
    for (auto i : pagemap) {
        std::cout << i << " ";
    }
    std::cout << endl;
}