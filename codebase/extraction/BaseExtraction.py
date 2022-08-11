import pyspark
from pyspark.sql import functions as F
from codebase.SparkSetup import SparkSetup

class BaseExtraction:

  def __init__(self) -> None:
    self.application = SparkSetup("Stock Analysis")
    self.spark = self.application.get_spark_instance()
