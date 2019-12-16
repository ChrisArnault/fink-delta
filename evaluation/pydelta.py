
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
            t = '0h0m{:.3f}s'.format(delta)
        elif delta < 3600:
            m = int(delta / 60)
            s = delta - (m*60)
            t = '0h{}m{:.3f}s'.format(m, s)
        else:
            h = int(delta / 3600)
            d = delta - (h*3600)
            m = int(d / 60)
            s = d - (m*60)
            t = '{}h{}h{:.3f}s'.format(h, m, s)

        print('--------------------------------', label, t)

        self.previous_time = now

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


def bench1(spark, conf):
    """
    Loop over batches.
    All batches with same schema

    :param spark:
    :param conf:
    :param batches:
    :return:
    """

    batch_size = conf.batch_size * conf.factor

    print("real batch_size={}".format(batch_size))

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

        os.system("hdfs dfs -du -h /user/chris.arnault/ | egrep {}".format(conf.dest))

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

    bench1(spark, conf)

    spark.stop()
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
