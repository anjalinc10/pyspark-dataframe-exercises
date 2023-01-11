from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "app5")
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

spark.sql("create database if not exists retail")
orderDf.write.format("csv")\
    .mode("overwrite")\
    .saveAsTable("retail.order3")


