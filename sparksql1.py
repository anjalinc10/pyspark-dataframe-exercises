from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "app4")
my_conf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week11\\orders.csv")\
    .load()

orderDf.createOrReplaceTempView("orders")

resultDf = spark.sql("select order_customer_id, count(*) as total_order from orders where "
                     "order_status = 'CLOSED' group by order_customer_id order by total_order")

resultDf.show()



