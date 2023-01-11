from pyspark import SparkConf
from pyspark.sql import SparkSession, Window


myconf = SparkConf()
myconf.set("spark.app.name", "Window_Aggregation")
myconf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=myconf).getOrCreate()

invoiceDF = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("header", True)\
    .option("path", "C:\\Users\\hp\\Desktop\\week12\\windowdata.csv")\
    .load()

myWindow = Window.partitionBy("country")\
    .orderBy("weeknum")\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

myDf = invoiceDF.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))

myDf.show()


