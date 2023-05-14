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
 
ENTRY_SIZE = 8 

def rmdir(path):
    command = "rm -rf {}".format(path)
    print(">>>>> Executing command {}".format(command))
    process = subprocess.Popen(command, stdout = subprocess.PIPE, 
        stderr = subprocess.STDOUT, shell=True,
        universal_newlines = True)
    return_code = process.wait()
    output = process.stdout.read()

    if return_code == 1:
        print("failed to execute command = ", command)
        print(output)
        exit()


def execute_command(command):
    print(">>>>> Executing command {}".format(command))
    process = subprocess.Popen(command, stdout = subprocess.PIPE, 
        stderr = subprocess.STDOUT, shell=True,
        universal_newlines = True)
    return_code = process.wait()
    output = process.stdout.read()

    if return_code == 1:
        print("failed to execute command = ", command)
        print(output)
        exit()

    print(output.split('\n'))
    return output.split('\n')
