import logging
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from spark_apps.data_transformations.med_qa.question_answering import (
    question_answering_transformer,
)

# /opt/spark/bin/spark-submit --master spark://0.0.0.0:7077  ./spark_apps/jobs/medqa_question_answering_embedding.py /opt/spark-data/med-qa-dataset/dataset/questions/ /opt/spark-data/med-qa-dataset/dataset/questions_transfomed_refat/ 

LOG_FILENAME = "project_medqa.log"
APP_NAME = "MedQA Pipeline: Question Answering Embedding"

if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    arguments = sys.argv
    print(f"Argument list passed: {arguments}")
    print(f"length of argument = {len(arguments)}")

    if len(arguments) != 3:
        logging.warning("Dataset file path and output path not specified!")
        sys.exit(1)

    dataset_path = arguments[1]
    output_path = arguments[2]

    conf = (
        SparkConf()
        .setAppName(APP_NAME)
        .set("spark.sql.warehouse.dir", "file:////opt/spark/apps/tmp")
        .set("spark.jars", "file:////opt/spark/apps/jars/*")
        .set("spark.driver.memory", "4g")
        .set("spark.executor.memory", "8g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set(
            "spark.sql.delta.catalogImpl", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession(spark)
    logging.info("Application Initialized: " + str(spark.sparkContext.appName))
    question_answering_transformer.run(spark, dataset_path, output_path)
    logging.info("Application Done: " + str(spark.sparkContext.appName))

    spark.stop()
