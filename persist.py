import logging
import logging.config
import sys


class Persist:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):
        try:
            logger = logging.getLogger("Persist")
            logger.info('Persisting')
            df.coalesce(1).write.option("header", "true").csv("transformed_retailstore.csv")
        except Exception as exp:
            logger.error("An error occured while persisting data >" + str(exp))
            # store in database table
            # send an email notification
            raise Exception("HDFS directory already exists")
