from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
myConf = SparkConf()
myConf.set("spark.app.local", "SQLexp")
myConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=myConf).getOrCreate()

orderDf = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week12\\dataset.dataset1")\
    .load()

df1 = orderDf.toDF("name","age","city")

def ageCheck(age):
    if(age > 18):
        return "Y"
    else:
        return "N"

spark.udf.register("parseAgeFunction", ageCheck)
for x in spark.catalog.listFunctions():
    print(x)

df2 = df1.withColumn("adult", expr("parseAgeFunction(age)"))
df2.show()

