import os
from pyspark import SparkConf, SparkContext

# SUBMIT_ARGS = "--spark.jars /opt/spark/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar pyspark"
# os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["HUDI_CONF_DIR"] = "file:////opt/spark/apps/conf"

# HUDI setup
# conf = (
#     SparkConf()
#     .setMaster("local[*]")
#     .setAppName("Hudi Spark Application")
#     .set("spark.driver.memory", "15g")
#     .set("spark.executor.memory", "15g")
#     .set("spark.sql.warehouse.dir", "file:////opt/spark/apps/tmp")
#     .set("spark.jars", "/opt/spark/apps/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar")
#     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#     .set(
#         "spark.sql.catalog.spark_catalog",
#         "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
#     )
#     .set(
#         "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
#     )
#     .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
#     .set("spark.sql.hive.convertMetastoreParquet", "true")
# )

conf = (
    SparkConf()
    .setMaster("local[*]")
    .setAppName("Delta Lake Spark Application")
    .set("spark.driver.memory", "30g")
    .set("spark.executor.memory", "30g")
    .set("spark.sql.warehouse.dir", "file:////opt/spark/apps/tmp")
    .set("spark.jars", "file:////opt/spark/apps/jars/*")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .set(
        "spark.sql.delta.catalogImpl", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

# builder = (
#     SparkSession.builder.appName("Delta Lake Spark Application")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config(
#         "spark.sql.catalog.spark_catalog",
#         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#     )
#     .config("spark.driver.memory", "15g")
#     .config("spark.executor.memory", "15g")
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# )


SPARK = SparkContext(conf=conf).getOrCreate()
# SPARK = configure_spark_with_delta_pip(builder).getOrCreate()
