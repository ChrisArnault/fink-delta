Evaluation for Delta Lake
=========================

To start developing
> conda deactivate
> d:\workspace\Script\dash-env\Script\activate.bat

> python dash-asynchronous.py

https://delta.io/
https://docs.delta.io/latest/index.html

hdfs dfs -du -h /lsst/DC2/cosmoDC2/xyz_v1.1.4_hive

spark-submit --master mesos://vm-75063.lal.in2p3.fr:5050
web ui -> https://wuip.lal.in2p3.fr:24444/

Scripts:
--------

bench.sh
check.sh

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx5g"

org.influxdb:influxdb-java:2.14

/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet
df = spark.read.format("parquet").load("/user/julien.peloton/xyz_v1.1.4.parquet")

scala
-----
val df = spark.read.format("parquet").load("/user/julien.peloton/xyz_v1.1.4.parquet")
df.show
df.write.format("delta").save("/user/chris.arnault/xyz")
val df2 = spark.read.format("delta").load("/user/chris.arnault/xyz")
df.show

val df = spark.read.format("parquet").load("/lsst/DC2/cosmoDC2/xyz_v1.1.4_hive")
df.show
df.write.format("delta").save("/user/chris.arnault/xyz2")
val df2 = spark.read.format("delta").load("/user/chris.arnault/xyz2")
df.show

/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet
val df = spark.read.format("parquet").load("/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet")
df.write.format("delta").save("/user/chris.arnault/xyz3")

supervision.lal.in2p3.fr/
http://vm-75222.lal.in2p3.fr/


History management
------------------

from delta.tables import *
table = DeltaTable.forPath(spark, dest)
h = table.history()
h.show()
h.filter("version < 10").show()

