

#include "include/database.h"

#include <cstdio>
#include <time.h>


int main (int argc, char * argv[])
{
    int data_size = atoi(argv[1]);
    Database db = Database();
    db_config_t config = {   
        .memtable_size = 1000,
        .eviction_algo = LRU,
        .use_bufferpool = false,
        .bufferpool_capacity = 100,
        .sst_type = BINARY_SEARCH
    };
    db.Open("db_experiment_step_1", config);
    
    for (int i = 0; i < data_size; i++) {
        db.Put(i, i);
    }

    struct timespec start, stop;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < 100; i++) {
        db.Put(i, i);
    }

    clock_gettime(CLOCK_MONOTONIC, &stop);

    printf("time=%.6lf\n",
            (stop.tv_sec - start.tv_sec)
            +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
            );


    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < 100; i++) {
        db.Get(i);
    }

    clock_gettime(CLOCK_MONOTONIC, &stop);

    printf("time=%.6lf\n",
            (stop.tv_sec - start.tv_sec)
            +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
            );


    clock_gettime(CLOCK_MONOTONIC, &start);

    db.Scan(0, 100);

    clock_gettime(CLOCK_MONOTONIC, &stop);

    printf("time=%.6lf\n",
            (stop.tv_sec - start.tv_sec)
            +(double)(stop.tv_nsec - start.tv_nsec) / 1000000000
            );

    db.Close();
}