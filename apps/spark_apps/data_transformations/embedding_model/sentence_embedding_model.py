# import torch
# import random
# from typing import Any, Dict, List
# from pyspark.sql import SparkSession, DataFrame, Row
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType, ArrayType, FloatType


from abc import ABC, abstractmethod
from typing import List


class SentenceEmbeddingModel(ABC):
    """Abstract class representing a sentence embedding model."""

    @abstractmethod
    def get_sentence_embedding(self, _: str) -> List[float]:
        """Embeds the preprocessed sentence into a vector."""
        raise NotImplementedError
