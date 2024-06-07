import logging
import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from spark_apps.data_transformations.med_qa.question_answering import (
    question_answering_transformer,
)

LOG_FILENAME = "project_medqa.log"
APP_NAME = "MedQA Pipeline: Question Answering Embedding"

if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    os.environ["HUDI_CONF_DIR"] = "file:////opt/spark/conf/hudi-defaults.conf"
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
        .set("spark.jars", "/opt/spark/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .set(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.sql.hive.convertMetastoreParquet", "false")
    )

    spark: SparkSession = SparkContext(conf=conf).getOrCreate()
    logging.info("Application Initialized: " + str(spark.sparkContext.appName))
    question_answering_transformer.run(spark, dataset_path, output_path)
    logging.info("Application Done: " + str(spark.sparkContext.appName))

    spark.stop()
