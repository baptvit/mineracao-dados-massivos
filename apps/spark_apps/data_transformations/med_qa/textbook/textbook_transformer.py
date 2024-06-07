from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import udf, col, concat, lit
from spark_apps.data_transformations.embedding_model.sentence_embedding_bert_model import (
    BertSentenceEmbedding,
)
from spark_apps.data_transformations.embedding_model.sentence_embedding_model import (
    SentenceEmbeddingModel,
)


def preproccess_textbook(
    _: SparkSession,
    dataframe: DataFrame,
    model: SentenceEmbeddingModel = BertSentenceEmbedding,
) -> DataFrame:

    embed_sentece_udf = udf(
        lambda sentence: model.get_sentence_embedding(sentence),
        ArrayType(ArrayType(DoubleType())),
    )

    dataframe = dataframe.withColumn(
        "embedding_sentence", embed_sentece_udf("sentence")
    )

    # Use the "embedding" column for further analysis
    dataframe.select("embedding_sentence").show()

    return dataframe


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = preproccess_textbook(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")
