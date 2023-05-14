#include "include/database.h"

#include <cstdio>
#include <time.h>

void query_workload(Database &db, int data_vol, int workload_type) {
   for (int i = 0; i< data_vol; i++) {
       
       if (workload_type == 0) {
        // datasets where clock perform better
        db.Get(0);
       } else {
        // datasets where lru perform better
        key_t random_key = 1 << (rand() % 15);
        db.Get(random_key);
       }
   }
}


int main (int argc, char * argv[])
{
    int bufferpool_size = atoi(argv[1]);
    int data_vol = atoi(argv[2]);
    int workload = atoi(argv[3]);

    // Clock algo
    Database db = Database();
    db_config_t config = {   
        .memtable_size = 100,
        .eviction_algo = CLOCK,
        .use_bufferpool = true,
        .bufferpool_capacity = bufferpool_size,
        .sst_type = BINARY_SEARCH,
        .bits_per_entry = 0,
        .storage_type = NAIVE
    };
    db.Open("db_experiment_step_2_clock", config);

    for (int i = 0; i < data_vol; i++) {
        db.Put(i,i);
    }

    struct timespec start, stop;
    clock_gettime(CLOCK_MONOTONIC, &start);
    query_workload(db, data_vol, workload);
    clock_gettime(CLOCK_MONOTONIC, &stop);
    printf("%.4lf\n",
                (stop.tv_sec - start.tv_sec)
                +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                );

    db.Close();

    // LRU algo
    db = Database();
    config = {   
        .memtable_size = 100,
        .eviction_algo = LRU,
        .use_bufferpool = true,
        .bufferpool_capacity = bufferpool_size,
        .sst_type = BINARY_SEARCH,
        .bits_per_entry = 0,
        .storage_type = NAIVE
    };
    db.Open("db_experiment_step_2_lru", config);

    for (int i = 0; i < data_vol; i++) {
        db.Put(i,i);
    }

    clock_gettime(CLOCK_MONOTONIC, &start);
    query_workload(db, data_vol, workload);
    clock_gettime(CLOCK_MONOTONIC, &stop);
    printf("%.4lf\n",
                (stop.tv_sec - start.tv_sec)
                +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
                );
    
    db.Close();
}
