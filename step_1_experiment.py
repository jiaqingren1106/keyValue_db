import subprocess
import os
import re
from collections import defaultdict
from textwrap import wrap
import pickle
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from plot import *

SIZE = [1000, 2000, 5000, 10000, 20000]
PUT_DATAPATH = "data_put.txt"
GET_DATAPATH = "data_get.txt"
SCAN_DATAPATH = "data_scan.txt"

def run_and_format(rep, command):
    put = []
    get = []
    scan = []
    put_total = 0
    get_total = 0
    scan_total = 0
    for s in SIZE:
        for i in range(rep):
            size_str = " {}".format(s)
            output = execute_command(command + size_str)
            # print(output[0][5:])
            put_total += float(output[0][5:])
            get_total += float(output[1][5:])
            scan_total += float(output[2][5:])
            rmdir("db_experiment_step_1")
        put.append(put_total/rep)
        get.append(get_total/rep)
        scan.append(scan_total/rep)
    print((put, get, scan))
    return (put, get, scan)

def save(tup):
    file0 = open(PUT_DATAPATH, "w+")
    file1 = open(GET_DATAPATH, "w+")
    file2 = open(SCAN_DATAPATH, "w+")
    # Saving the array in a text file
    for i in range(len(tup[0])):
        data_str = str(SIZE[i] * ENTRY_SIZE) + " " + str(100 * ENTRY_SIZE/tup[0][i]) + "\n"
        file0.write(data_str)
        data_str = str(SIZE[i] * ENTRY_SIZE) + " " + str(100 * ENTRY_SIZE/tup[1][i]) + "\n"
        file1.write(data_str)
        data_str = str(SIZE[i] * ENTRY_SIZE) + " " + str(100 * ENTRY_SIZE/tup[2][i]) + "\n"
        file2.write(data_str)
    file0.close()
    file1.close()
    file2.close()

def perf_run():
    data = run_and_format(5,"./experiment_step_1")
    save(data)
    graph()


def graph():
    size, tp = np.loadtxt(GET_DATAPATH, usecols=(0, 1), unpack=True)
    plt.plot(size, tp, label="Get")
    plt.xlabel('data volume(B)')
    plt.ylabel('throughPut(B/S)')
    plt.legend()
    plt.title("get throughPut with different data volume")
    plt.savefig("get.png")
    plt.clf()

    size, tp = np.loadtxt(PUT_DATAPATH, usecols=(0, 1), unpack=True)
    plt.plot(size, tp, label="Put")
    plt.xlabel('data volume(B)')
    plt.ylabel('throughPut(B/S)')
    plt.legend()
    plt.title("put throughPut with different data volume")
    plt.savefig("put.png")
    plt.clf()

    size, tp = np.loadtxt(SCAN_DATAPATH, usecols=(0, 1), unpack=True)
    plt.plot(size, tp, label="Scan")
    plt.xlabel('data volume(B)')
    plt.ylabel('throughPut(B/S)')
    plt.legend()
    plt.title("scan throughPut with different data volume")
    plt.savefig("scan.png")


if __name__ == "__main__":
    perf_run()
