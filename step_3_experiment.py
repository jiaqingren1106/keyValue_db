import plot
from plot import ENTRY_SIZE
import matplotlib.pyplot as plt

def run_experiment_lsm():
    plot.rmdir("db_experiment_step_3_lsm")
    num_per_op = 100
    num_every_interval = 1000000
    output = plot.execute_command("./experiment_step_3_lsm " 
                                    + str(num_per_op)
                                    + " " + str(100*1024*1024)
                                    + " " + str(num_every_interval))
    # print("execution finished")
    bytes_per_op = num_per_op*ENTRY_SIZE
    put_y = []
    get_y = []
    scan_y = []
    x = []
    for i, line in enumerate(output):
        if line == "":
            continue
        times = line.split(" ")
        put_y += [bytes_per_op/float(times[0])]
        get_y += [bytes_per_op/float(times[1])]
        scan_y += [bytes_per_op/float(times[2])]
        x += [i*num_every_interval*ENTRY_SIZE]
    
    # exclude the first one since it is when data volum is 0
    plt.plot(x[1:], put_y[1:], label="Put")
    plt.title("Put throughput with increasing data size")
    plt.xlabel("data size (B)")
    plt.ylabel("Throughput (B/s)")
    plt.savefig("put_experiment_step_3.png")
    plt.clf()
    
    plt.plot(x[1:], get_y[1:], label="Get")
    plt.title("Get throughput with increasing data size")
    plt.xlabel("data size (B)")
    plt.ylabel("Throughput (B/s)")
    plt.savefig("get_experiment_step_3.png")
    plt.clf()
    
    plt.plot(x[1:], scan_y[1:], label="Scan")
    plt.title("Scan throughput with increasing data size")
    plt.xlabel("data size (B)")
    plt.ylabel("Throughput (B/s)")
    plt.savefig("scan_experiment_step_3.png")
    plt.clf()

def run_experiment_bloom():
    num_per_op = 100
    x = [1,2,3,4,5,6,7,8,9,10]
    y = []
    for bit in x:
        plot.rmdir("db_experiment_step_3_bloom")
        output = plot.execute_command("./experiment_step_3_bloom " 
                                        + str(bit) 
                                        + " " + str(num_per_op)
                                        + " " + str(50*1024*1024))
        bytes_per_op = num_per_op*ENTRY_SIZE
        time = float(output[0])
        y += [bytes_per_op / time]
    plt.plot(x,y)
    plt.title("Get performance as bloom filter bits change")
    plt.xlabel("Bloom filter bits per entry")
    plt.ylabel("Query throughput (B/s)")
    plt.savefig("bloom_filter_experiment_step_3.png")

if __name__ == "__main__":
    run_experiment_lsm()
    run_experiment_bloom()
