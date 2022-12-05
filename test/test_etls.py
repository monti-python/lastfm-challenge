from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql import window as W
from datetime import datetime
import pytest

from lastfm_pyspark.etls import assign_session_id


def test_assign_session_id(spark):
    schema = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
    ])
    df = spark.createDataFrame([
        ("user1", datetime.fromisoformat("2020-01-01 00:00:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:15:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:30:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:55:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:10:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:20:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:45:00")),
    ], schema=schema)
    df = assign_session_id(
        df,
        threshold=1200,
        user_col="user_id",
        time_col="timestamp"
    )
    expected = spark.createDataFrame([
        ("user1", datetime.fromisoformat("2020-01-01 00:00:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:15:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:30:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:55:00"), 1),
        ("user2", datetime.fromisoformat("2020-01-01 00:10:00"), 0),
        ("user2", datetime.fromisoformat("2020-01-01 00:20:00"), 0),
        ("user2", datetime.fromisoformat("2020-01-01 00:45:00"), 1),
    ], schema=schema.add("session_id", T.IntegerType()))
    assert df.collect() == expected.collect()