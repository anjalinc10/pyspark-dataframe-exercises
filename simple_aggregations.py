from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

myConf = SparkConf()
myConf.set("spark.app.name", "simple_Aggregation")
myConf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=myConf).getOrCreate()

invoiceDF = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("header", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week12\\order_data.csv")\
    .load()

# column Object Expression

invoiceDF.select(count("*").alias("RowCount"),
sum("Quantity").alias("TotalQuantity"),
avg("UnitPrice").alias("AvgPrice"),
countDistinct("invoiceNo").alias("CountDistinct")).show()

# column String expression

invoiceDF.selectExpr("count(*) as RowCount",
                     "sum(Quantity) as TotalQuantity",
                     "avg(UnitPrice) as AvgPrice",
                     "count(Distinct(InvoiceNo)) as CountDistinct").show()

# Spark SQL

invoiceDF.createOrReplaceTempView("sales")
resultDf = spark.sql("select count(*), sum(Quantity), "
          "avg(UnitPrice), count(distinct(InvoiceNo)) from sales")

resultDf.show()
