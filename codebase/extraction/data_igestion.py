import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("stock analysis").getOrCreate()

df = spark.read.csv(path="data\\NSE-TATAGLOBAL.csv",header =True)

df.show()