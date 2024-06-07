from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, concat, lit
from pyspark.sql.types import ArrayType, DoubleType

from spark_apps.data_transformations.embedding_model.sentence_embedding_bert_model import (
    BertSentenceEmbedding,
)
from spark_apps.data_transformations.embedding_model.sentence_embedding_model import (
    SentenceEmbeddingModel,
)


def transform_options(dataframe: DataFrame) -> DataFrame:
    """
    Transforms a Row object containing options (answer choices) into a single sentence.

    Args:
        options_row: A Row object with named fields representing answer choices.

    Returns:
        A string containing all options separated by commas and joined with "or".
    """
    return dataframe.withColumn(
        "options_sentence",
        concat(
            dataframe.options.getField("A"),
            lit(", "),
            dataframe.options.getField("B"),
            lit(", "),
            dataframe.options.getField("C"),
            lit(", "),
            dataframe.options.getField("D"),
            lit(" or "),
            dataframe.options.getField("E"),
        ),
    )


def create_sentece(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn(
        "sentence",
        concat(
            lit("The questions is: "),
            col("question"),
            lit(", Given the options: "),
            col("options_sentence"),
            lit(", The answer is: "),
            col("answer"),
        ),
    )


def preproccess_question_answering(
    _: SparkSession,
    dataframe: DataFrame,
    model: SentenceEmbeddingModel = BertSentenceEmbedding(),
) -> DataFrame:

    dataframe = transform_options(dataframe)
    dataframe = create_sentece(dataframe)

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
    # TO DO: Still peding implementation
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = preproccess_question_answering(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode="append")
