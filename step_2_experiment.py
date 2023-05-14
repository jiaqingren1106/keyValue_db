import plot
from plot import ENTRY_SIZE
import matplotlib.pyplot as plt


def run_experiment_eviction_workload(workload):
   x = [1,10,50,100,200,500]
   data_vol = 5000
   clock_y = []
   lru_y = []
   for size in x:
       lru_total = 0
       clock_total = 0
       for i in range(5):
           plot.rmdir("db_experiment_step_2_clock")
           plot.rmdir("db_experiment_step_2_lru")
           output = plot.execute_command(
                       "./experiment_step_2_eviction "
                       + str(size) + " " + str(data_vol)
                       + " " + str(workload))
           clock_total += float(output[0])
           lru_total += float(output[1])
       clock_total /= 5
       lru_total /= 5


       clock_y += [data_vol/clock_total]
       lru_y += [data_vol/lru_total]
   plt.plot(x, clock_y, label="Clock")
   plt.plot(x, lru_y, label="LRU")
   plt.xlabel('buffer pool size')
   plt.ylabel('throughPut')
   plt.legend()
   plt.title("get throughPut with different buffer pool size under 5000 data volume")
   plt.savefig("lru_vs_clock_workload_{}.png".format(workload))
   plt.clf()

def run_experiment_eviction():
    run_experiment_eviction_workload(0)
    run_experiment_eviction_workload(1)

def run_experiment_sst():
    x = [5000,10000,20000,50000,100000,150000,200000]
    real_x = []
    bin_y = []
    btree_y = []
    num_per_query = 1000
    for size in x:
        plot.rmdir("db_experiment_step_2_binary")
        plot.rmdir("db_experiment_step_2_btree")
        output = plot.execute_command(
                    "./experiment_step_2_sst "
                    + str(size)
                    + " " + str(num_per_query))
        bin_time = float(output[0])
        btree_time = float(output[1])
        
        bin_y += [num_per_query*ENTRY_SIZE / bin_time]
        btree_y += [num_per_query*ENTRY_SIZE / btree_time]
        
        real_x += [size*ENTRY_SIZE]
    
    plt.plot(real_x, bin_y, label="Binary search")
    plt.plot(real_x, btree_y, label="B-Tree")
    plt.title("Binary search VS B-Tree indexing query performance")
    plt.xlabel("data size (B)")
    plt.ylabel("Get throughput (B/s)")
    plt.legend()
    plt.savefig("binary_search_vs_b_tree.png")

if __name__ == "__main__":
    run_experiment_eviction()
    run_experiment_sst()
