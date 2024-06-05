# import torch
# import random
# from typing import Any, Dict, List
# from pyspark.sql import SparkSession, DataFrame, Row
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType, ArrayType, FloatType


# class SentenceEmbeddingModel:
#     """Abstract class representing a sentence embedding model."""

#     def preprocess(self, dataframe: DataFrame):
#         """Preprocesses the sentence for embedding."""
#         raise NotImplementedError

#     def embed(self, preprocessed_sentence):
#         """Embeds the preprocessed sentence into a vector."""
#         raise NotImplementedError


# class UniversalSentenceEncoder(SentenceEmbeddingModel):
#     """Concrete class for Universal Sentence Encoder (USE) embedding."""

#     # (Implementation details for USE preprocessing and embedding logic)

#     def preprocess(self, sentence):
#         # ... (USE specific preprocessing)
#         return preprocessed_sentence

#     def embed(self, preprocessed_sentence):
#         # ... (USE specific embedding logic)
#         return vector_embedding


# class SentenceEmbeddingPipeline:
#     """
#     Class managing the selection and execution of a SentenceEmbeddingModel.
#     """

#     def __init__(self, model_type):
#         if not issubclass(model_type, SentenceEmbeddingModel):
#             raise ValueError(
#                 "Invalid model type. Must inherit from SentenceEmbeddingModel"
#             )
#         self.model = model_type()

#     def preprocess_and_embed(self, df, sentence_col="sentence", output_col="embedding"):
#         """
#         Preprocesses sentences and embeds them using the selected model.
#         """
#         preprocess_udf = udf(lambda s: self.model.preprocess(s), StringType())
#         embed_udf = udf(lambda s: self.model.embed(s), ArrayType(FloatType()))

#         df = df.withColumn("preprocessed_sentence", preprocess_udf(df[sentence_col]))
#         df = df.withColumn(output_col, embed_udf("preprocessed_sentence"))

#         return df.drop("preprocessed_sentence")


# # Usage example
# spark = SparkSession.builder.appName("SentenceEmbedding").getOrCreate()

# # Choose the model type
# model_type = BertSentenceEmbedding  # Or UniversalSentenceEncoder

# # Create embedding pipeline
# pipeline = SentenceEmbeddingPipeline(BertSentenceEmbedding)

# # Load your dataframe with a "sentence" column
# data = [("This is a sample sentence."), ...]
# df = spark.createDataFrame(data, ["sentence"])

# # Apply preprocessing and embedding
# df_embedded = pipeline.preprocess_and_embed(df)

# # Use the "embedding" column for further analysis
# df_embedded.show()
