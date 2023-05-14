#include "include/database.h"

#include <cstdio>
#include <time.h>

int main (int argc, char * argv[])
{
    int data_num = atoi(argv[1]);
    int query_num = atoi(argv[2]);
    
    
    // get the uniformly distributed keys to query
    // get the keys in advance to avoid random number generation overhead
    std::vector<key_t> keys_to_get;
    for (int i = 0; i < query_num; i++) {
        key_t k = rand() % data_num;
        keys_to_get.push_back(k);
    }

    // Binary search
    Database db = Database();
    db_config_t config = {   
        .memtable_size = 5000,
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = 100,
        .sst_type = BINARY_SEARCH,
        .bits_per_entry = 0,
        .storage_type = NAIVE
    };
    db.Open("db_experiment_step_2_binary", config);

    for (int i = 0; i < data_num; i++) {
        db.Put(i,i);
    }

    struct timespec start, stop;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (auto key : keys_to_get) {
        db.Get(key);
    }
    clock_gettime(CLOCK_MONOTONIC, &stop);
    printf("%.4lf\n",
                (stop.tv_sec - start.tv_sec)
                +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                );

    db.Close();

    // B-tree
    db = Database();
    config = {   
        .memtable_size = 5000,
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = 100,
        .sst_type = B_TREE,
        .bits_per_entry = 0,
        .storage_type = NAIVE
    };
    db.Open("db_experiment_step_2_btree", config);

    for (int i = 0; i < data_num; i++) {
        db.Put(i,i);
    }

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (auto key : keys_to_get) {
        db.Get(key);
    }
    clock_gettime(CLOCK_MONOTONIC, &stop);
    printf("%.4lf\n",
                (stop.tv_sec - start.tv_sec)
                +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                );

    db.Close();
}
