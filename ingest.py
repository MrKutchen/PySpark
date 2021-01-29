import logging
import logging.config


class Ingest:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')
        # customer_df = self.spark.read.csv("retailstore.csv", header=True)
        course_df = self.spark.sql("select * from udemycoursebb.ud_course_table")
        logger.info('DataFrame created')
        logger.warning("DataFrame created with warning")
        return course_df
