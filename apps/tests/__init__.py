from pyspark.sql import SparkSession

SPARK = SparkSession.builder.appName("UnitTests").config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
