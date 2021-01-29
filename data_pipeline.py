import logging
import logging.config
import sys

from pyspark.sql import SparkSession

import ingest
import persist
import transform


class Pipeline:
    logging.config.fileConfig("resources/configs/logging.conf")

    def run_pipeline(self):
        try:
            logging.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark)
            df = ingest_process.ingest_data()
            df.show()
            transform_process = transform.Transform(self.spark)
            transformed_df = transform_process.transform_data(df)
            transformed_df.show()
            persist_process = persist.Persist(self.spark)
            persist_process.persist_data(transformed_df)
            logging.info('run_pipeline method ended')
        except Exception as exp:
            logging.error("An error occured while running the pipeline > " + str(exp))
            # send email notification
            # log error to database
            sys.exit(1)

        return

    def create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("my first spark app") \
            .enableHiveSupport().getOrCreate()


if __name__ == '__main__':
    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')
