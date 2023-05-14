#pragma once

#include "type.h"
#include <vector>

class SimpleBufferPool {
    public:
        std::vector<kv_pair*> buffer;

        kv_pair* Get(key_t key);
        void Put(kv_pair* page);
        void Delete(kv_pair* page);
};