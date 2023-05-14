#include "include/database.h"

#include <cstdio>
#include <time.h>

int main (int argc, char * argv[])
{
    // M for bloom filter
    int bloom_bits = atoi(argv[1]);
    // number of entries to get to measure throughput
    int num_per_get = atoi(argv[2]);
    int total_bytes = atoi(argv[3]);

    Database db = Database();
    db_config_t config = {   
        .memtable_size = 1024*1024/sizeof(kv_pair), // 1MB
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = 10*1024*1024/PAGE_SIZE, // 10MB
        .sst_type = BINARY_SEARCH,
        .bits_per_entry = bloom_bits,
        .storage_type = LSM_TREE
    };
    db.Open("db_experiment_step_3_bloom", config);
    
    // 1GB of data
    int total_entries_inserted = total_bytes / sizeof(kv_pair);
//     int total_entries_inserted = (1024*1024) / sizeof(kv_pair);
    
    for (int i = 0; i < total_entries_inserted; i++) {
        db.Put(i, i);
    }
    
    // get the random keys for Bloom filter to query
    std::vector<key_t> keys_to_get;
    for (int i = 0; i < num_per_get; i++) {
        key_t k = rand() % total_entries_inserted;
        keys_to_get.push_back(k);
    }
 
    struct timespec start, stop;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (auto i : keys_to_get) {
        db.Get(i);
    }
    clock_gettime(CLOCK_MONOTONIC, &stop);
    printf("%.4lf\n",
                (stop.tv_sec - start.tv_sec)
                +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                );
}
