import os
os.sys.path.append(os. getcwd())
from codebase.extraction.BaseExtraction import BaseExtraction

class DataIngestion(BaseExtraction):
  
  def __init__(self) -> None:
    super().__init__()

  def process_stock_data(self):

    stock_data = self.spark.read.csv(path="data\\NSE-TATAGLOBAL.csv",header =True)
    stock_data.printSchema()

data_ingest = DataIngestion()
data_ingest.process_stock_data()