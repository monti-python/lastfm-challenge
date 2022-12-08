from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.context import SparkConf
import logging
import argparse

from datautils import assign_session_id, get_top_n_sessions


def get_spark_session() -> SparkSession:
    conf = (
        SparkConf()
        .setAppName("exploration")
        .setMaster("local[*]")
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "8g")
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
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


def main(output_dir: str):
    logging.info("Creating Spark session...")
    spark = get_spark_session()
    logging.info("Loading LastFM dataset...")
    raw_df = load_lastfm_dataset(spark)

    logging.info("Exporting top 10 songs in the top 50 sessions to tsv...")
    plays_df = assign_session_id(
        raw_df, threshold=1200, user_col="user_id", time_col="timestamp"
    )
    sessions_df = get_top_n_sessions(
        plays_df, session_key=["user_id", "session_id"], 
        time_col="timestamp", n=50
    )
    top10_df = (
        sessions_df
        .join(plays_df, on=["user_id", "session_id"], how="inner")
        .groupBy("track_id")
        .agg(F.count("track_id").alias("plays"))
        .orderBy(F.col("plays").desc())
        .limit(10)
    )
    top10_df.toPandas().to_csv(f"{output_dir}/top10.tsv", index=False, sep="\t")
    logging.info(f"Exported results to file '{output_dir}/top10.tsv'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", "-o", type=str, default="/out")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    main(args.output_dir)