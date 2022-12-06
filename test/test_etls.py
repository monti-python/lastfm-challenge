from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql import window as W
from datetime import datetime
import pytest

from lastfm_pyspark.etls import assign_session_id, get_top_n_sessions


def test_assign_session_id(spark):
    schema = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
    ])
    timeseries_df = spark.createDataFrame([
        ("user1", datetime.fromisoformat("2020-01-01 00:00:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:15:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:30:00")),
        ("user1", datetime.fromisoformat("2020-01-01 00:55:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:10:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:20:00")),
        ("user2", datetime.fromisoformat("2020-01-01 00:45:00")),
    ], schema=schema)
    res = assign_session_id(
        timeseries_df,
        threshold=1200,
        user_col="user_id",
        time_col="timestamp"
    )
    exp = spark.createDataFrame([
        ("user1", datetime.fromisoformat("2020-01-01 00:00:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:15:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:30:00"), 0),
        ("user1", datetime.fromisoformat("2020-01-01 00:55:00"), 1),
        ("user2", datetime.fromisoformat("2020-01-01 00:10:00"), 0),
        ("user2", datetime.fromisoformat("2020-01-01 00:20:00"), 0),
        ("user2", datetime.fromisoformat("2020-01-01 00:45:00"), 1),
    ], schema=schema.add("session_id", T.IntegerType()))
    assert res.collect() == exp.collect()

def test_get_top_n_sessions(spark):
    schema_in = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("session_id", T.IntegerType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
    ])
    schema_out = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("session_id", T.IntegerType(), True),
        T.StructField("session_duration", T.LongType(), True),
    ])

    df = spark.createDataFrame([
        ("user1", 0, datetime.fromisoformat("2020-01-01 00:00:00")),
        ("user1", 0, datetime.fromisoformat("2020-01-01 00:15:00")),
        ("user1", 0, datetime.fromisoformat("2020-01-01 00:30:00")),
        ("user1", 1, datetime.fromisoformat("2020-01-01 00:55:00")),
        ("user2", 0, datetime.fromisoformat("2020-01-01 00:10:00")),
        ("user2", 0, datetime.fromisoformat("2020-01-01 00:20:00")),
        ("user2", 1, datetime.fromisoformat("2020-01-01 00:45:00")),
    ], schema=schema_in)

    res = get_top_n_sessions(
        df,
        session_key=["user_id", "session_id"],
        time_col="timestamp",
        n=2
    )
    exp = spark.createDataFrame([
        ("user1", 0, 60*30),
        ("user2", 0, 60*10),
    ], schema=schema_out)
    assert res.collect() == exp.collect()