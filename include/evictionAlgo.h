#pragma once

class EvictionAlgo {
public:
        virtual int accessAndUpdate(int pageNum) = 0;
        virtual void evict(int pageNum) = 0;
};