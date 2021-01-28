import pyspark
from pyspark.sql import SparkSession


class Transform:
    def __init__(self,spark):
        self.spark=spark

    def transform_data(self,df):
        print("Transforming")
        # drop all the rows having null values
        df1 = df.na.drop()
        return df1