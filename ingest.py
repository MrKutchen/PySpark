import logging
import logging.config

import pandas
import pandas.io.sql as sqlio
import psycopg2
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


class Ingest:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')
        # customer_df = self.spark.read.csv("retailstore.csv",header=True)
        course_df = self.spark.sql("select * from fxxcoursedb.fx_course_table")
        logger.info('DataFrame created')
        logger.warning('DataFrame created with warning')
        return course_df

    def read_from_pg(self):
        connection = psycopg2.connect(user='postgres', password='Jekyll', host='localhost', database='postgres')
        cursor = connection.cursor()
        sql_query = "select * from futurexschema.futurex_course_catalog"
        pdDF = sqlio.read_sql_query(sql_query, connection)
        sparkDf = self.spark.createDataFrame(pdDF)
        sparkDf.show()

    def read_from_pg_using_jdbc_driver(self):

        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "futurexschema.futurex_course_catalog") \
            .option("user", "postgres") \
            .option("password", "Jekyll") \
            .load()

        jdbcDF.show()
