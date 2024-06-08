# from pyspark.sql import SparkSession
# from pyspark import SparkConf, SparkContext
# import os

# # SUBMIT_ARGS = "--spark.jars /opt/spark/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar pyspark"
# # os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
# os.environ["HUDI_CONF_DIR"] = "file:////opt/spark/conf/hudi-defaults.conf"

# conf = (
#     SparkConf()
#     .setMaster("local[*]")
#     .setAppName("Hudi Spark Application")
#     .set("spark.driver.memory", "15g")
#     .set("spark.executor.memory", "15g")
#     .set("spark.jars", "/opt/spark/apps/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar")
#     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#     .set(
#         "spark.sql.catalog.spark_catalog",
#         "org.apache.spark.sql.hudi.catalog.SuperiorHoodieCatalog",
#     )
#     .set(
#         "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
#     )
#     .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
#     .set("spark.sql.hive.convertMetastoreParquet", "false")
# )
# SPARK_HUDI = SparkContext(conf=conf).getOrCreate()

# # SPARK_HUDI = SparkSession.builder \
# #     .master("local") \
# #     .appName("Hudi Spark Application") \
# #     .config("spark.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12-0.14.1.jar") \
# #     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
# #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
# #     .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
# #     .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
# #     .config('spark.sql.hive.convertMetastoreParquet', 'false') \
# #     .getOrCreate()
