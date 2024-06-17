import os
from typing import List
from apps.spark_apps.data_transformations.similarity_search.similarity_search import SimilaritySearch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, concat, lit
from pyspark.sql.types import ArrayType, DoubleType

def evaluate_filtered_dataframe(dataframe: DataFrame)

def evaluate_question_answering(
    _: SparkSession,
    dataframe: DataFrame,
    list_test: List,
    evaluation_report_path: str,
) -> None:
    
    # for each test I will return top 10 similar records and evaluate top1, top5 and top10 in batch

    # 1 - Interate in list of test (which is a List of Dict with sentences rewrited)
    # 2 - Compare the rewrited sentence with each entry in database.
    # 3 - Return top 10 most similar records
    # 4 - Compare within the top 10 dataframe returned is indeed the expected result.
    # 5 - Generate a report comparing, Euclidian distance, dot product, cosine similarity.
    # 6 - Generate a report with accurancy, recall, precision and f1 score. for each distance calculated
    similarity_search = SimilaritySearch()
    for teste_case in list_test:
        top10_dataframe = similarity_search.search(teste_case["rewrited_vector_embedding"], dataframe, 10)
        teste_case["rewrited_vector_embedding"]
        # TO DO: Peding finishing
        
    return None


def run(
    spark: SparkSession, input_dataset_path: str, test_dataset_path: str, evaluation_report_path: str
) -> None:
    input_dataset = spark.read.format("delta").load(input_dataset_path)
    test_dataset = spark.read.json(test_dataset_path)
    input_dataset.cache()
    test_dataset.cache()
    input_dataset.show()
    test_dataset.show()

    evaluate_question_answering(spark, input_dataset, evaluation_report_path)
