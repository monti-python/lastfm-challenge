from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.context import SparkConf
import logging

from data_utils import assign_session_id, get_top_n_sessions


def get_spark_session() -> SparkSession:
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
    return spark


def load_lastfm_dataset(spark):
    schema = T.StructType(
        [
            T.StructField("user_id", T.StringType(), True),
            T.StructField("timestamp", T.TimestampType(), True),
            T.StructField("artist_id", T.StringType(), True),
            T.StructField("artist_name", T.StringType(), True),
            T.StructField("track_id", T.StringType(), True),
            T.StructField("track_name", T.StringType(), True),
        ]
    )
    return spark.read.csv(
        "lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv",
        sep="\t",
        header=False,
        schema=schema,
    )


def main():
    logging.info("Creating Spark session...")
    spark = get_spark_session()
    logging.info("Loading LastFM dataset...")
    raw_df = load_lastfm_dataset(spark)

    logging.info("Computing top 10 songs in the top 50 sessions...")
    plays_df = assign_session_id(
        raw_df, threshold=1200, user_col="user_id", time_col="timestamp"
    )
    sessions_df = get_top_n_sessions(
        plays_df, session_key=["user_id", "session_id"], 
        time_col="timestamp", top=50
    )
    top10_df = (
        sessions_df
        .join(plays_df, on=["user_id", "session_id"], how="inner")
        .groupBy("track_id")
        .agg(F.count("track_id").alias("plays"))
        .orderBy(F.col("plays").desc())
        .limit(10)
    )
    logging.info("Exporting results to tsv file 'top10.tsv'")
    top10_df.write.csv("/out/top10.tsv", header=True, sep="\t")


if __name__ == "__main__":
    main()