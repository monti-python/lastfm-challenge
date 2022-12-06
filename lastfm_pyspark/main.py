from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import window as W
from pyspark.context import SparkConf

from etls import assign_session_id, get_top_n_sessions

conf = (
    SparkConf()
    .setAppName("exploration")
    .setMaster("local[*]")
    .set("spark.executor.memory", "4g")
    .set("spark.driver.memory", "4g")
    .set("spark.driver.maxResultSize", "2g")
    .set("spark.sql.execution.arrow.enabled", "true")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()

schema = T.StructType([
    T.StructField("user_id", T.StringType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("artist_id", T.StringType(), True),
    T.StructField("artist_name", T.StringType(), True),
    T.StructField("track_id", T.StringType(), True),
    T.StructField("track_name", T.StringType(), True),
])

raw_df = spark.read.csv(
    "lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv",
    sep="\t",
    header=False,
    schema=schema,
)

# What are the top 10 songs played in the top 50 longest sessions by tracks count?

plays_df = assign_session_id(
    raw_df, threshold=1200, user_col="user_id", time_col="timestamp"
)

sessions_df = get_top_n_sessions(
    plays_df, session_key=["user_id", "session_id"], 
    time_col="timestamp", top=50
)

top10 = (
    sessions_df.join(plays_df, on=['user_id', 'session_id'], how="inner")
    .groupBy('track_id')
    .agg(
        F.count('track_id').alias('plays'),
    )
    .orderBy(F.col('plays').desc())
    .limit(10)
)
