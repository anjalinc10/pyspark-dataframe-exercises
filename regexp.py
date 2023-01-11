from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

my_conf = SparkConf()
my_conf.set("spark.app.name", "regexp")
my_conf.set("spark.master", "local[*]")

spark = SparkSession\
    .builder\
    .config(conf=my_conf)\
    .getOrCreate()

my_regex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'
lines_df = spark.read.text("C:\\Users\\hp\\Desktop\\week11\\orders_new.csv")

final_df = lines_df.select(
    regexp_extract('value', my_regex, 1).alias("order_id"),
    regexp_extract('value', my_regex, 2).alias("date"),
    regexp_extract('value', my_regex, 3).alias("customer_id"),
    regexp_extract('value', my_regex, 4).alias("status"))

# final_df.printSchema()
final_df.show()
final_df.groupby("status").count().show()



