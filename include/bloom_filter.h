#pragma once

#include "type.h"

#include "xxhash32.h"


class BloomFilter {
private:
    uint32_t HashKey(key_t key, int k) {
        return XXHash32::hash(&key, sizeof(key_t), k);
    }
    int num_bits;
    int optimal_k;
public:
    BloomFilter(int num_entries, int bits_per_entry) {
        this->num_bits = num_entries * bits_per_entry;
        this->optimal_k = 0.693 * bits_per_entry; // ln2 * M
        if (bits_per_entry > 0 && this->optimal_k == 0) {
            this->optimal_k = 1;
        }
    }
    bool Query(key_t key, uint8_t* filter) {
        for (int i = 0; i < this->optimal_k; i++) {
            auto hash_val = this->HashKey(key, i);
            int bit_pos = hash_val % this->num_bits;
            uint8_t mask = 128 >> (bit_pos%8);
            uint8_t hit = filter[bit_pos/8] & mask;
            if (!hit) {
                return false;
            }
        }
        return true;
    }
    void Insert(key_t key, uint8_t* filter) {
        for (int i = 0; i < this->optimal_k; i++) {
            auto hash_val = this->HashKey(key, i);
            int bit_pos = hash_val % this->num_bits;
            uint8_t mask = 128 >> (bit_pos%8);
            filter[bit_pos/8] |= mask;
        }
    }
};
