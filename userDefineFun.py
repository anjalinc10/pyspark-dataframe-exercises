from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "UDF")
my_conf.set("spark.master", "local[*]")

spark = SparkSession\
    .builder\
    .config(conf=my_conf)\
    .getOrCreate()

df = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week12\\dataset.dataset1")\
    .load()

df1 = df.toDF("name", "age", "city")

def ageCheck(age):
    if(age>18):
        return "Y"
    else:
        return "N"

parseAgeFunction = udf(ageCheck)
df2 = df1.withColumn("adult", parseAgeFunction("age"))
df2.printSchema()
df2.show()


