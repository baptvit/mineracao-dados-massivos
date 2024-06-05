from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, expr, col, array, struct
from pyspark.sql.types import ArrayType, DoubleType, StringType

from spark_apps.data_transformations.embedding_model.sentence_embedding_bert_model import (
    BertSentenceEmbedding,
)

def transform_options(options_row: Dict[str, Any]):
  """
  Transforms a Row object containing options (answer choices) into a single sentence.

  Args:
      options_row: A Row object with named fields representing answer choices.

  Returns:
      A string containing all options separated by commas and joined with "or".
  """
  # Extract option values using element extraction
  options = [col(field_name) for field_name in options_row.asDict()]
  # Concatenate options with commas and join with "or" using CONCAT_WS and expr
  return expr("concat_ws(',', ", array(*options), ") OR ")

def preproccess_question_answering(
    _spark: SparkSession, dataframe: DataFrame
) -> DataFrame:

    # Define a UDF (User Defined Function) to extract options as a sentence
    extract_options_udf = udf(lambda row: ", ".join(row.asDict().values()), StringType())

    # Apply the UDF to the "text_dict" column to create a new column "options_text"
    dataframe = dataframe.withColumn("options", extract_options_udf(dataframe["options"]))
    
    #dataframe = dataframe.withColumn("options_text", transform_options(col("options")))

    # pipeline = SentenceEmbeddingPipeline(BertSentenceEmbedding)

    model = BertSentenceEmbedding()
    # preprocess_udf = udf(lambda row: model.preprocess(row), StringType())
    embed_udf = udf(
        lambda row: model.bert_embedding_sentece(row), ArrayType(ArrayType(DoubleType()))
    )

    dataframe = dataframe.withColumn("embedding_sentence", embed_udf(struct([dataframe[x] for x in dataframe.columns])))

    # Use the "embedding" column for further analysis
    dataframe.select("embedding_sentence").show(truncate=False)

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
