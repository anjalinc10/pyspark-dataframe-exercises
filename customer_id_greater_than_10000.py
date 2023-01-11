from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "OrdersApp")
my_conf.set("spark.master", "local[*]")

spark = SparkSession\
    .builder\
    .config(conf=my_conf)\
    .getOrCreate()

orderDf = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("C:\\Users\\hp\\Desktop\\week11\\orders.csv")

groupDf = orderDf.repartition(4)\
    .where("order_customer_id > 10000")\
    .select("order_id", "order_customer_id")\
    .groupBy("order_customer_id")\
    .count()
groupDf.show()

# orderDf.show()

spark.stop()
