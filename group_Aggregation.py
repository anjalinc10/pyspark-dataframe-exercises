from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

myconf = SparkConf()
myconf.set("spark.app.name", "group_Aggregation")
myconf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=myconf).getOrCreate()

invoiceDF = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("header", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week12\\order_data.csv")\
    .load()

# column object expression
resultDF = invoiceDF.groupBy("Country", "InvoiceNo")\
    .agg(sum("Quantity").alias("TotalQuantity"),
         sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))
resultDF.show()

# column string expression
resultDF = invoiceDF.groupBy("Country", "InvoiceNo")\
    .agg(expr("sum(Quantity) as TotalQuantity"),
expr("sum(Quantity * UnitPrice) as InvoiceValue"))
resultDF.show()

# spark SQL
invoiceDF.createOrReplaceTempView("sales")
spark.sql(""" select country, InvoiceNo, sum(Quantity) as totQty, sum(Quantity * UnitPrice)
as InvoiceValue from sales group by country, InvoiceNo""").show()
