from pyspark import SparkConf
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope='session')
def spark():
    conf = SparkConf().setAll([
        ('spark.default.parallelism', '1'),
        ('spark.driver.memory', '1g'),
        ('spark.executor.memory', '1g'),
        ('spark.ui.enabled', 'false'),
        ('spark.driver.maxResultSize', '1g'),
    ])
    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
    yield spark
    spark.stop()