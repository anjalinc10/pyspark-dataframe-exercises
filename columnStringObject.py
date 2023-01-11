from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "columnStringObject")
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

# orderDf.select("order_id", "order_date").show()
orderDf.select(col("order_id"), col("order_date")).show()
