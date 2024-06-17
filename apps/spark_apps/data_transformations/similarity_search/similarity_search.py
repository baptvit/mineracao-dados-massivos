from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.ml.linalg import Vectors

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.ml.linalg import Vectors

VECTOR_COL = "embedding_column"

class SimilaritySearch():
  """
  Abstract class for similarity search algorithms in PySpark.
  """

  def __init__(self, metric : str ="COSINE"):
    """
    Constructor for the SimilaritySearch class.

    Args:
      metric (str, optional): The similarity metric to use. Defaults to "COSINE".
    """
    self.metric: str = metric

    self.metric_functions = {
    "COSINE": self._cosine_similarity_udf,
    "L2": self._euclidean_distance_udf,
    "IP": self._inner_product_udf,
    }
    # Get the UDF function based on the chosen metric
    metric = metric.upper()  # Ensure case-insensitive metric selection
    if metric in self.metric_functions:
      self.similarity_udf = udf(self.metric_functions[metric], FloatType())
    else:
      raise NotImplementedError("Unsupported metric: {}".format(metric))


  def _cosine_similarity_udf(self, vec_base, vec_test) -> float:
    """
    Implementation for cosine similarity metric.

    Args:
      vec_base (pyspark.ml.linalg.DenseVector): The first vector.
      vec_test (pyspark.ml.linalg.DenseVector): The second vector.

    Returns:
      float: The cosine similarity score between the vectors.
    """
    return float(vec_base.dot(vec_test) / (vec_base.norm(p=2) * vec_test.norm(p=2)))
  
  def _inner_product_udf(self, vec_base, vec_test):
    return float(vec_base.dot(vec_test))
  
  def _euclidean_distance_udf(self, vec_base, vec_test):
    """
    Calculates the Euclidean distance between two vectors.

    Args:
        vec1 (pyspark.ml.linalg.DenseVector): First vector.
        vec2 (pyspark.ml.linalg.DenseVector): Second vector.

    Returns:
        float: Euclidean distance between the vectors.
    """
    # Ensure both vectors have the same size
    if vec_base.size != vec_test.size:
      raise ValueError("Vectors must have the same size")
    
    # Calculate squared differences of elements
    squared_diffs = [(x - y) ** 2 for x, y in zip(vec_base.toArray(), vec_test.toArray())]
    
    # Return the square root of the sum of squared differences
    return float(sum(squared_diffs)) ** 0.5

  def search(self, query_vector, data, k):
    """
    Searches for the top K most similar vectors to the query vector in the data.

    Args:
      query_vector (pyspark.ml.linalg.DenseVector): The query vector.
      data (pyspark.sql.DataFrame): The DataFrame containing vectors.
      k (int): The number of nearest neighbors to search for.

    Returns:
      pyspark.sql.DataFrame: A DataFrame containing the top K most similar vectors.
    """
    # Calculate similarity with all vectors in the DataFrame
    data_with_sim = data.withColumn("similarity", self.similarity_udf(data.col(VECTOR_COL), query_vector))
    # Sort by similarity in descending order and select top K
    return data_with_sim.sort(data_with_sim.col("similarity").desc()).limit(k)