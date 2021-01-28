import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

class Ingest:

    def __init__(self,spark):
        self.spark=spark

    def ingest_data(self):
        print("Ingesting from csv")
        customer_df = self.spark.read.csv("retailstore.csv",header=True)

        return customer_df
