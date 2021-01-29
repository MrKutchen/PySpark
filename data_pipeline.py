import logging
import logging.config
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import ingest
import persist
import transform


class Pipeline:
    logging.config.fileConfig("resources/configs/logging.conf")

    def run_pipeline(self):
        try:
            logging.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark)
            ingest_process.read_from_pg()
            df = ingest_process.ingest_data()
            df.show()
            transform_process = transform.Transform(self.spark)
            transformed_df = transform_process.transform_data(df)
            transformed_df.show()
            persist_process = persist.Persist(self.spark)
            persist_process.insert_into_pg()
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

    def create_hive_table(self):
        # self.spark.sql("create database if not exists fxxcoursedb")
        # self.spark.sql("create table if not exists fxxcoursedb.fx_course_table (course_id string,course_name string,author_name string,no_of_reviews string)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (1,'Java','FutureX',45)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")
        # Treat empty strings as null
        self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")


if __name__ == '__main__':
    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    # pipeline.create_hive_table()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')
