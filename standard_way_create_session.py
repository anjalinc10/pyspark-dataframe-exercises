from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession\
    .builder\
    .config(conf=my_conf)\
    .getOrCreate()
orderDf = spark.read.format("csv")\
     .option("header", True)\
     .option("inferSchema", True)\
     .option("path", "C:\\Users\\hp\\Desktop\\week11\\orders.csv")\
     .load()

print("number of partitions are", orderDf.rdd.getNumPartitions())
#
orderDf.repartition(4).write.format("csv")\
    .mode("overwrite")\
    .option("path", "C:\\Users\\hp\\Desktop\\week11\\output")\
    .save()
