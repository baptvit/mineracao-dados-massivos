from pyspark.sql import SparkSession, DataFrame


def preproccess_question_answering(
    _spark: SparkSession, dataframe: DataFrame
) -> DataFrame:
    return dataframe


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    # TO DO: Still peding implementation
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = preproccess_question_answering(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")
