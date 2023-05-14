
#include "include/database.h"

#include <cstdio>
#include <time.h>

int main (int argc, char * argv[])
{
    // number of entries to get/put/scan in order to measure throughput
    int num_per_op = atoi(argv[1]);
    int total_bytes = atoi(argv[2]);
    int interval = atoi(argv[3]);
    
    Database db = Database();
    db_config_t config = {   
        .memtable_size = 1024*1024/sizeof(kv_pair), // 1MB
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = 10*1024*1024/PAGE_SIZE, // 10MB
        .sst_type = BINARY_SEARCH,
        .bits_per_entry = 5,
        .storage_type = LSM_TREE
    };
    db.Open("db_experiment_step_3_lsm", config);


    // 1GB of data
    int total_entries_inserted = total_bytes / sizeof(kv_pair);

    int pair_inserted = 0;
    while( pair_inserted < total_entries_inserted) {
        
        
        struct timespec start, stop;
        /* Put throughput */
        // measure put time
        clock_gettime(CLOCK_MONOTONIC, &start);  
        for (int i = 0; i < num_per_op; i++) {
            db.Put(i, i);
        }
        clock_gettime(CLOCK_MONOTONIC, &stop);
        printf("%.6lf ",
                    (stop.tv_sec - start.tv_sec)
                    +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                    );
        
        /* Get throughput */
        // get the random keys to query
        std::vector<key_t> keys_to_get;
        for (int i = 0; i < num_per_op; i++) {
            key_t k = rand() % total_entries_inserted;
            keys_to_get.push_back(k);
        }
        // measure get time
        clock_gettime(CLOCK_MONOTONIC, &start);
        for (auto i : keys_to_get) {
            db.Get(i);
        }
        clock_gettime(CLOCK_MONOTONIC, &stop);
        printf("%.6lf ",
                    (stop.tv_sec - start.tv_sec)
                    +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                    );
        
        /*Scan throughput*/
        // random scan start
        int random_start = rand() % (total_entries_inserted - num_per_op);
        // measure scan time
        clock_gettime(CLOCK_MONOTONIC, &start);
        db.Scan(random_start, random_start + num_per_op-1);
        clock_gettime(CLOCK_MONOTONIC, &stop);
        printf("%.6lf\n",
                    (stop.tv_sec - start.tv_sec)
                    +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                    );
        
        
        for (int i = pair_inserted; i < pair_inserted+interval; i++) {
            db.Put(i, i);
        }
        pair_inserted += interval;
    }

    db.Close();
}
