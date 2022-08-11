import os
os.sys.path.append(os. getcwd())
from codebase.extraction.BaseExtraction import BaseExtraction
from pyspark.sql.types import StructType,StructField,LongType,StringType, IntegerType,DateType,DoubleType

class DataIngestion(BaseExtraction):
  
  schema = StructType([ \
    StructField("date",DateType(),True), \
    StructField("open",DoubleType(),True), \
    StructField("high",DoubleType(),True), \
    StructField("low", DoubleType(), True), \
    StructField("last", DoubleType(), True), \
    StructField("close", DoubleType(), True), \
    StructField("total_trade_quantity", LongType(), True), \
    StructField("turnover_lacs", DoubleType(), True)
  ])

  def __init__(self) -> None:
    super().__init__()

  def process_stock_data(self):

    stock_data = self.spark.read.schema(self.schema).csv(path="data\\NSE-TATAGLOBAL.csv",header =True)
    stock_data.printSchema()
    stock_data.show()

data_ingest = DataIngestion()
data_ingest.process_stock_data()