
import sys
import os
import random
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Conf(object):
    def __init__(self):
        self.factor = 100000
        self.batch_size = 1
        self.partitions = 1000
        # file_format = "parquet"
        self.file_format = "delta"
        self.dest = "/user/chris.arnault/xyz"
        self.loops = 10

    def set(self):
        for i, arg in enumerate(sys.argv[1:]):
            a = arg.split("=")
            print(i, arg, a)
            if a[0] == "batch_size":
                self.batch_size = int(a[1])
            if a[0] == "factor":
                self.factor = int(a[1])
            if a[0] == "partitions":
                self.partitions = int(a[1])
            if a[0] == "file_format":
                self.file_format = a[1]
            if a[0] == "loops":
                self.loops = int(a[1])

        self.dest = "{}_{}".format(self.dest, self.file_format)
        print("file_name={}".format(self.dest))


class Stepper(object):
    previous_time = None

    def __init__(self):
        self.previous_time = time.time()

    def show_step(self, label='Initial time'):
        now = time.time()
        delta = now - self.previous_time

        if delta < 60:
            t = '0d0h0m{:.3f}s'.format(delta)
        elif delta < 3600:
            m = int(delta / 60)
            s = delta - (m*60)
            t = '0d0h{}m{:.3f}s'.format(m, s)
        else:
            d = int(delta / (24*3600))
            delta = delta - (d*24*3600)
            h = int(delta / 3600)
            delta = delta - (h*3600)
            m = int(delta / 60)
            delta = delta - (m*60)
            s = delta
            t = '{}d{}h{}m{:.3f}s'.format(d, h, m, s)

        print('--------------------------------', label, '|', t)

        self.previous_time = now
rn
        return delta


ra_offset = 40.0
ra_field = 40.0
dec_offset = 20.0
dec_field = 40.0
z_offset = 0.0
z_field = 5.0


def ra_value():
    return ra_offset + random.random() * ra_field


def dec_value():
    return dec_offset + random.random() * dec_field


def z_value():
    return z_offset + random.random() * z_field

def any_value():
    return 0 + random.random() * 10.0


def get_file_size(conf):
    import subprocess
    cmd = "hdfs dfs -du -h /user/chris.arnault/ | egrep {}".format(conf.dest)
    result = subprocess.check_output(cmd, shell=True).decode().split("\n")
    for line in result:
        if conf.dest in line:
            a = line.split()
            size = float(a[0])
            scale = a[1]
            if scale == 'K':
                size *= 1.0/1024.0
            elif scale == 'M':
                size *= 1
            elif scale == 'G':
                size *= 1024
            return size
    return 0


def bench1(spark, conf):
    """
    Loop over batches.
    All batches with same schema

    :param spark:
    :param conf:
    :return:
    """

    batch_size = conf.batch_size * conf.factor

    print("real batch_size={}".format(batch_size))

    previous_size = 0

    for batch in range(conf.loops):
        print("batch #{}".format(batch))

        s = Stepper()
        values = [(ra_value(), dec_value(), z_value()) for i in range(batch_size)]
        df = spark.createDataFrame(values, ['ra', 'dec', 'z'])
        df = df.repartition(conf.partitions, "ra")
        df = df.cache()
        df.count()
        s.show_step("building the dataframe")

        s = Stepper()
        if batch == 0:
            df.write.format(conf.file_format).save(conf.dest)
        else:
            df.write.format(conf.file_format).mode("append").save(conf.dest)
        s.show_step("Write block")

        new_size = get_file_size(conf)
        increment = new_size - previous_size
        previous_size = new_size

        print("file_size={} increment={}".format(new_size, increment))

    s = Stepper()
    df = spark.read.format(conf.file_format).load(conf.dest)
    s.show_step("Read file")
    parts = df.rdd.getNumPartitions()
    print("partitions = {}".format(parts))

    df.show()


def bench2(spark, conf):
    """
    Loop over batches.
    Variable schema at every row

    :param spark:
    :param conf:
    :return:
    """

    batch_size = conf.batch_size * conf.factor

    print("real batch_size={}".format(batch_size))

    names = "azertyuiopqsdfghjklmwxcvbn1234567890"

    previous_size = 0

    column = lambda : 0 + random.random() * 10.0

    first = True
    for batch in range(conf.loops):
        print("batch #{}".format(batch))

        s0 = Stepper()

        total_rows = 0
        while total_rows < batch_size:
            # s = Stepper()
            columns = random.randint(3, len(names))
            rows = random.randint(1, conf.factor/10)
            total_rows += rows

            values = [[column() for n in range(columns)] for i in range(1, rows)]
            column_names = [names[i] for i in range(columns)]
            try:
                df = spark.createDataFrame(values, column_names)
            except:
                print("bad frame")
            # s.show_step("    building the dataframe with rows={} for {} total_rows={}".format(rows, column_names, total_rows))

            # s = Stepper()
            if first:
                df.write.format(conf.file_format).option("mergeSchema", "true").save(conf.dest)
                first = False
            else:
                df.write.format(conf.file_format).mode("overwrite").option("mergeSchema", "true").mode("append").save(conf.dest)
            # s.show_step("    Write little block total_rows={} rows={} columns={}".format(total_rows, rows, columns))

        new_size = get_file_size(conf)
        increment = new_size - previous_size
        previous_size = new_size

        s0.show_step("file_size={} increment={} rows={}".format(new_size, increment, total_rows))

    s = Stepper()
    df = spark.read.format(conf.file_format).load(conf.dest)
    s.show_step("Read file")
    parts = df.rdd.getNumPartitions()
    print("partitions = {}".format(parts))

    df.show()


if __name__ == "__main__":
    conf = Conf()
    conf.set()

    s = Stepper()
    spark = SparkSession.builder.appName("Delta").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    s.show_step("init")

    s = Stepper()
    os.system("hdfs dfs -rm -r -f {}".format(conf.dest))
    s.show_step("erase the file")

    print("============= create the DF with ra|dec|z")

    print("factor={}".format(conf.factor))
    print("batch_size={}".format(conf.batch_size))
    print("loops={}".format(conf.loops))

    bench2(spark, conf)

    # spark.stop()
    exit()

"""
    flux_field = 10.0

    print("============= add the column for flux")
    df = df.withColumn('flux', flux_field * rand())
    df.write.format(file_format).mode("append").option("mergeSchema", "true").save(dest)

    names = "azertyuiopqsdfghjklmwxcvbn1234567890"
    print("============= add {} columns".format(len(names)))
    for c in names:
        df = df.withColumn(c, rand())

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dest)

    print("============= add a column for SN tags")
    df.withColumn('SN', when(df.flux > 8, True).otherwise(False))

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dest)

    os.system("hdfs dfs -du -h {}".format(dest))

    df = spark.read.format("delta").load(dest)
    df.show()
    print("count = {} partitions={}".format(df.count(), df.rdd.getNumPartitions()))


import pandas as pd
import matplotlib.pyplot as plt

x = [random.random() for i in range(1000)]
df = pd.DataFrame({'x': x,})
hist = df.hist(bins=10)
plt.show()

"""
