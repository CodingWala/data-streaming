from pyspark.sql import SparkSession

class SparkSetup:

  def __init__(self,application_name) -> None:
    self.application_name = application_name

  def get_spark_instance(self)->object:
    spark = SparkSession.builder.appName(self.application_name).getOrCreate()
    return spark