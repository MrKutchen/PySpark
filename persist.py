import logging
import logging.config
import sys

import psycopg2
import pyspark
from pyspark.sql import SparkSession


class Persist:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):
        try:
            logger = logging.getLogger("Persist")
            logger.info('Persisting')
            # df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")

        except Exception as exp:
            logger.error("An error occured while persisiting data >" + str(exp))
            # store in database table
            # send an email notification
            raise Exception("HDFS directory already exists")

    def insert_into_pg(self):
        connection = psycopg2.connect(user='postgres', password='Jekyll', host='localhost', database='postgres')
        cursor = connection.cursor()
        insert_query = "INSERT INTO futurexschema.futurex_course_catalog (course_id, course_name, author_name, course_section, creation_date) VALUES (%s, %s, %s, %s,%s)"
        insert_tuple = (3, 'Machine Learning', 'FutureX', '{}', '2020-10-20')
        cursor.execute(insert_query, insert_tuple)
        cursor.close()
        connection.commit()
