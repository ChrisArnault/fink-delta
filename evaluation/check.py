
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

data = []

colors = ["r", "g", "b", "c", "m", "y", "k"]

def check(num, file_name):
    global data

    with open(file_name) as f:
        content = f.readlines()

    content = [x.strip() for x in content]

    values = []
    for line in content:
        if "Write block" in line:
            line = line.split("Write block 0h0m")[1]
            x = line.split("s")
            values.append(float(x[0]))

    # values = [22.581, 20.183, 19.148, 18.585, 17.347, 19.156, 15.076, 22.181, 20.943, 19.537]
    print(values)


    if len(values) > 0:
        import subprocess
        cmd = "hdfs dfs -du -h /user/chris.arnault/"
        result = subprocess.check_output(cmd, shell=True).decode().split("\n")
        for line in result:
            if "/user/chris.arnault/xyz" in line:
                a = line.split()
                size = float(a[0])
                scale = a[1]
                if scale == 'G':
                    size *= 1024
                print(size)
                block = size/len(values)
                print(block)

        values = [block/v for v in values]
        y_mean = [np.mean(values)]*len(values)
        print(values, y_mean)
        data.append(values)
        plt.plot(values, label=file_name, color=colors[num])
        plt.plot(y_mean, color=colors[num])



if __name__ == "__main__":
    log = sys.argv

    for i, arg in enumerate(sys.argv[1:]):
        check(i, arg)

    plt.ylabel("Mo/s")
    plt.xlabel("batch #")
    plt.legend()
    plt.show()
