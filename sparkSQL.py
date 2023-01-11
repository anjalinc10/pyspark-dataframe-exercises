from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "app3")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
orderDf = spark.read.format("csv")\
    .option("header", True)\
    .option("inferScheme", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week11\\orders.csv")\
    .load()

orderDf.createOrReplaceTempView("orders")\

resultDf = spark.sql("select order_status,count(*) as total_orders from orders group by order_status")
resultDf.show()
