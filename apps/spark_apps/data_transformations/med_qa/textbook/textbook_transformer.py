import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import udf, col, concat, lit
from spark_apps.data_transformations.embedding_model.sentence_embedding_bert_model import (
    BertSentenceEmbedding,
)
from spark_apps.data_transformations.embedding_model.sentence_embedding_model import (
    SentenceEmbeddingModel,
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


def preproccess_textbook(
    _: SparkSession,
    dataframe: DataFrame,
    model: SentenceEmbeddingModel = BertSentenceEmbedding(),
) -> DataFrame:

    embed_sentece_udf = udf(
        lambda sentence: model.get_sentence_embedding(sentence),
        ArrayType(ArrayType(DoubleType())),
    )

    dataframe = dataframe.filter(col("value") != "")
    dataframe = transform_metadata(dataframe)
    dataframe = dataframe.withColumnRenamed("value", "sentence")

    dataframe = dataframe.withColumn(
        "embedding_sentence", embed_sentece_udf("sentence")
    )

    dataframe = dataframe.withColumn(
        "embedding_sentence", dataframe["embedding_sentence"].getItem(0)
    )
    return dataframe.select("sentence", "embedding_sentence", "metadata")


def run(
    spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str
) -> None:
    input_dataset = spark.read.text(input_dataset_path).select("*", "_metadata")

    input_dataset.cache()
    input_dataset.show()

    dataset_transformer = preproccess_textbook(spark, input_dataset)
    dataset_transformer.cache()
    dataset_transformer.show()

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
