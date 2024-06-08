import os
import shutil

from pyspark.sql import SparkSession, DataFrame
from spark_apps.data_transformations.med_qa.textbook.textbook_transformer import run
from tests import SPARK


SAMPLE_DATA = [
    {"value": "What is anatomy?"},
    {"value": ""},
    {
        "value": "Anatomy includes those structures that can be seen grossly (without the aid of magnification) and microscopically (with the aid of magnification). Typically, when used by itself, the term anatomy tends to mean gross or macroscopic anatomy—that is, the study of structures that can be seen without using a microscopic. Microscopic anatomy, also called histology, is the study of cells and tissues using a microscope."
    },
    {"value": ""},
    {
        "value": "Anatomy forms the basis for the practice of medicine. Anatomy leads the physician toward an understanding of a patient’s disease, whether he or she is carrying out a physical examination or using the most advanced imaging techniques. Anatomy is also important for dentists, chiropractors, physical therapists, and all others involved in any aspect of patient treatment that begins with an analysis of clinical signs. The ability to interpret a clinical observation correctly is therefore the endpoint of a sound anatomical understanding."
    },
    {"value": ""},
    {
        "value": "Observation and visualization are the primary techniques a student should use to learn anatomy. Anatomy is much more than just memorization of lists of names. Although the language of anatomy is important, the network of information needed to visualize the position of physical structures in a patient goes far beyond simple memorization. Knowing the names of the various branches of the external carotid artery is not the same as being able to visualize the course of the lingual artery from its origin in the neck to its termination in the tongue. Similarly, understanding the organization of the soft palate, how it is related to the oral and nasal cavities, and how it moves during swallowing is very different from being able to recite the names of its individual muscles and nerves. An understanding of anatomy requires an understanding of the context in which the terminology can be remembered."
    },
    {"value": ""},
    {"value": "How can gross anatomy be studied?"},
    {"value": ""},
]


def test_run_textbook_hudi() -> None:
    input_textbook_path = "tmp/mock_textbook"
    transformed_dataset_path = "tmp/transformed_dataset_path"
    spark: SparkSession = SparkSession(SPARK)

    df: DataFrame = spark.createDataFrame(SAMPLE_DATA)
    df.write.mode("overwrite").text(input_textbook_path)

    run(spark, input_textbook_path, transformed_dataset_path)

    df_transformed = spark.read.format("hudi").load(
        f"file:///{os.path.abspath(transformed_dataset_path)}"
    )
    assert len(df_transformed.columns) == 7
    assert df_transformed.count() == 5

    shutil.rmtree("tmp/")
