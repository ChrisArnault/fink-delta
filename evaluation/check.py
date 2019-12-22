
import os
import sys
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


colors = ["r", "g", "b", "c", "m", "y", "k"]


class Value(object):
    def __init__(self, batch):
        self.batch = batch
        self.time = 0
        self.size = 0

    def __str__(self):
        return "time={} zize={}".format(self.time, self.size)


def check(num, file_name):
    with open(file_name) as f:
        content = f.readlines()

    content = [x.strip() for x in content]

    values = []

    start_batch = False
    value = None

    keys = ("batch #", "| ", "increment=")

    for line in content:
        if "batch #" in line:
            batch = int(line.split(keys[0])[1])
            value = Value(batch)
            start_batch = True
        if start_batch and (keys[1] in line):
            t = line.split(keys[1])[1]
            m = re.match("([0-9]+)[h]([0-9]+)m([0-9]+)[.]([0-9]+)[s]", t)
            value.time = float(m.group(1))*3600.0 + float(m.group(2))*60.0 + float(m.group(3)) + float(m.group(4))
            # print("time={}".format(value.time))
        if start_batch and (keys[2] in line):
            t = line.split(keys[2])[1]
            t = t.split(" ")[0]
            size = float(t)
            value.size = size
            values.append(value)
            start_batch = False

    # values = [22.581, 20.183, 19.148, 18.585, 17.347, 19.156, 15.076, 22.181, 20.943, 19.537]
    # print("values=", values)


    if len(values) > 0:
        values = [v.size/v.time for v in values]
        y_mean = [np.mean(values[1:])]*len(values)
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

