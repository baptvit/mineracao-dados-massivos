import logging
import sys
import os 


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from spark_apps.data_transformations.med_qa.textbook import textbook_transformer

LOG_FILENAME = "project_medqa.log"
APP_NAME = "MedQA Pipeline: Text Book Embedding"

# /opt/spark/bin/spark-submit --master spark://0.0.0.0:7077  ./spark_apps/jobs/medqa_textbook_embedding.py /opt/spark-data/med-qa-dataset/dataset/textbook/ /opt/spark-data/med-qa-dataset/dataset/textbook_transfomed/ 
# /opt/spark/bin/spark-submit --master spark://0.0.0.0:7077  ./spark_apps/jobs/medqa_textbook_embedding.py /opt/spark-data/med-qa-dataset/dataset/textbook/ /opt/spark-data/med-qa-dataset/dataset/textbook_transfomed/

def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    files = list(map(lambda x: os.path.join(os.path.abspath(input_dataset_path), x),os.listdir(input_dataset_path)))
    input_dataset = spark.read.text(files).select("*", "_metadata")

    #input_dataset.cache()
    #input_dataset.show()

    dataset_transformer = textbook_transformer.preproccess_textbook(spark, input_dataset)
    #dataset_transformer.cache()
    #dataset_transformer.show()

    ## Write as HUDI
    # hudi_options = {
    #     "hoodie.table.name": "textbook_pre_proccess",
    #     "hoodie.datasource.write.hive_style_partitioning": "true",
    #     "hoodie.datasource.write.table.name": "textbook_pre_proccess",
    #     "hoodie.upsert.shuffle.parallelism": 1,
    #     "hoodie.insert.shuffle.parallelism": 1,
    #     "hoodie.consistency.check.enabled": True,
    #     "hoodie.index.type": "BLOOM",
    #     "hoodie.index.bloom.num_entries": 60000,
    #     "hoodie.index.bloom.fpp": 0.000000001,
    #     "hoodie.cleaner.commits.retained": 2,
    # }

    # dataset_transformer.write.format("hudi").options(**hudi_options).mode(
    #     "overwrite"
    # ).save(f"file:///{os.path.abspath(transformed_dataset_path)}")

    ## Write as Delta
    dataset_transformer.write.format("delta").mode("overwrite").save(
        f"file:///{os.path.abspath(transformed_dataset_path)}"
    )

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
        .set("spark.executor.memory", "5g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set(
            "spark.sql.delta.catalogImpl", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark: SparkSession = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession(spark)
    logging.info("Application Initialized: " + str(spark.sparkContext.appName))
    run(spark, dataset_path, output_path)
    logging.info("Application Done: " + str(spark.sparkContext.appName))

    spark.stop()
