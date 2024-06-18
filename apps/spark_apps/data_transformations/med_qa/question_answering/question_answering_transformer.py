import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, concat, lit
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StructType,
    StructField,
)

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


def transform_metadata(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn(
        "metadata",
        concat(
            lit("Raw file name is: "),
            dataframe._metadata.getField("file_name"),
            lit(", Were read at: "),
            dataframe._metadata.getField("file_modification_time"),
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

    dataframe = transform_metadata(dataframe)
    dataframe = transform_options(dataframe)
    dataframe = create_sentece(dataframe)
    dataframe = dataframe.filter((col("sentence") != "") & (col("sentence").isNotNull()))

    schema = StructType(
        [
            StructField(
                "embedding_sentence", ArrayType(ArrayType(DoubleType())), False
            ),
            StructField("token_sentence", IntegerType(), False),
        ]
    )

    embed_sentece_udf = udf(
        lambda sentence: model.get_sentence_embedding(sentence),
        schema,
    )

    dataframe = dataframe.withColumn(
        "embedding_sentence_object", embed_sentece_udf("sentence")
    )

    dataframe = dataframe.withColumn(
        "embedding_sentence",
        dataframe["embedding_sentence_object"]
        .getField("embedding_sentence")
        .getItem(0),
    ).withColumn(
        "token_sentence",
        dataframe["embedding_sentence_object"].getField("token_sentence"),
    )

    dataframe = dataframe.drop_duplicates()
    return dataframe.select(
        "sentence", "embedding_sentence", "token_sentence", "metadata"
    )


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    input_dataset = spark.read.json(input_dataset_path).select("*", "_metadata")
    #input_dataset.cache()
    #input_dataset.show()

    dataset_transformer = preproccess_question_answering(spark, input_dataset)
    #dataset_transformer.cache()
    #dataset_transformer.show()

    # hudi_options = {
    #     "hoodie.table.name": "question_answering_pre_proccess",
    #     "hoodie.datasource.write.hive_style_partitioning": "true",
    #     "hoodie.datasource.write.table.name": "question_answering_pre_proccess",
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

    dataset_transformer.write.format("delta").mode("overwrite").save(
        f"file:///{os.path.abspath(transformed_dataset_path)}"
    )
