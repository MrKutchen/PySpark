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

    # def create_hive_table(self):
    #     self.spark.sql("create database if not exists udemycoursebb")
    #     self.spark.sql("create table if not exists udemycoursebb.ud_course_table(course_id string, course_name string, author_name string, no_of_reviews string)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (1, 'JAVA', 'Udemy', null)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (2, 'JAVA', null, 56)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (3, 'Big Data', 'Udemy', 123)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (4, 'Linux', 'Udemy', 93)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (5, 'Microservices', 'Udemy', 120)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (6, 'CMS', 'Udemy', 23)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (7, 'Dot Net', 'Udemy', 93)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (8, 'Python', null, null)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (9, 'CMS', 'Udemy', 58)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (10, 'Ansible', 'Udemy', null)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (11, 'Jenkins', 'Udemy', 45)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (12, 'Go Lang', null, 57)")
    #     self.spark.sql("insert into udemycoursebb.ud_course_table VALUES (13, 'Swift', 'Udemy', 86)")
    #     #Treat empty strings as null values
    #     self.spark.sql("alter table udemycoursebb.ud_course_table set tblproperties('serialization.null.format'='')")




if __name__ == '__main__':
    logging.info('Application started')
    pipeline = Pipeline()
    pipeline.create_spark_session()
    # pipeline.create_hive_table()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')
