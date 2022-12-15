import logging
from typing import List

from constants import SESSION_PATH, SESSION_SCHEMA
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    collect_list,
    count,
    desc,
    expr,
    lag,
    max,
    min,
    round,
    sum,
)
from pyspark.sql.types import Row
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
conf = SparkConf()


# this needs to be configured if running locally and depending on machine specs
# to avoid warnings similar to
# https://stackoverflow.com/questions/46907447/meaning-of-apache-spark-warning-calling-spill-on-rowbasedkeyvaluebatch
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")


def read_session_data(spark: SparkSession) -> DataFrame:
    """
    Read session and user profile data and join on userid.
    :param spark: SparkSession
    :return: Spark DataFrame
    """
    logger.info("Reading data from %s", SESSION_PATH)
    data = (
        spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(SESSION_SCHEMA)
        .load(SESSION_PATH)
    )
    cols_to_drop = ("artistid", "trackid")
    return data.drop(*cols_to_drop).cache()


def create_users_and_distinct_songs_count(df: DataFrame) -> List[Row]:
    """
    Create a list of user IDs, along with the number of distinct songs each user has played.
    :param df: Spark DataFrame
    :return: List
    """
    df1 = df.select("userid", "artistname", "trackname").dropDuplicates()
    df2 = (
        df1.groupBy("userid")
        .agg(count("*").alias("DistinctTrackCount"))
        .orderBy(desc("DistinctTrackCount"))
    )
    return df2.collect()


def create_popular_songs(df: DataFrame, limit=100) -> List[Row]:
    """
    Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of
    times each was played..
    :param df: Spark DataFrame
    :param limit: int
    :return: List
    """
    df1 = (
        df.groupBy("artistname", "trackname")
        .agg(count("*").alias("CountPlayed"))
        .orderBy(desc("CountPlayed"))
        .limit(limit)
    )
    return df1.collect()


def create_session_ids_for_all_users(
    df: DataFrame, session_cutoff: int
) -> DataFrame:
    """
    Creates a new 'session_ids' for each user depending for a timestamp if time between successive
    played tracks exceeds session_cutoff.
    :param df: Spark DataFrame
    :param session_cutoff: int
    :return: Spark DataFrame
    """
    w1 = Window.partitionBy("userid").orderBy("timestamp")

    df1 = (
        df.withColumn("pretimestamp", lag("timestamp").over(w1))
        .withColumn(
            "delta_mins",
            round(
                (
                    col("timestamp").cast("long")
                    - col("pretimestamp").cast("long")
                )
                / 60
            ),
        )
        .withColumn(
            "sessionflag",
            expr(
                f"CASE WHEN delta_mins > {session_cutoff} OR delta_mins IS NULL THEN 1 ELSE 0 END"
            ),
        )
        .withColumn("sessionID", sum("sessionflag").over(w1))
    )

    return df1


def compute_top_n_longest_sessions(df: DataFrame, limit: int) -> DataFrame:
    """
    Calculates the length of each session for each user from difference of timestamps
    of first and last songs of each session. Returns top n longest sessions, where n is determined
    by the limit argument.
    :param df: Spark DataFrame
    :param limit: int
    :return: Spark DataFrame
    """
    df1 = (
        df.groupBy("userid", "sessionID")
        .agg(
            min("timestamp").alias("session_start_ts"),
            max("timestamp").alias("session_end_ts"),
        )
        .withColumn(
            "session_length(hrs)",
            round(
                (
                    col("session_end_ts").cast("long")
                    - col("session_start_ts").cast("long")
                )
                / 3600
            ),
        )
        .orderBy(desc("session_length(hrs)"))
        .limit(limit)
    )

    return df1


def longest_sessions_with_tracklist(
    df: DataFrame, session_cutoff: int = 20, limit: int = 10
) -> DataFrame:
    """
    Creates a dataframe of the top 10 longest sessions (by elapsed time), with the following information about
    each session: userid, timestamp of first and last songs in the  session, and the list of songs played
    in the session (in order of play). An exsiting session ends if time between successive
    tracks exceeds session_cutoff.
    :param df: Spark DataFrame
    :param session_cutoff:int, Default:20
    :param limit: int, Default:10
    :return: DataFrame
    """
    df1 = create_session_ids_for_all_users(df, session_cutoff)
    df2 = compute_top_n_longest_sessions(df1, limit)
    df3 = (
        df1.join(df2, ["userid", "sessionID"])
        .select("userid", "sessionID", "trackname", "session_length(hrs)")
        .groupBy("userid", "sessionID", "session_length(hrs)")
        .agg(collect_list("trackname").alias("tracklist"))
        .orderBy(desc("session_length(hrs)"))
    )
    return df3


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("lastfm").config(conf=conf).getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    df = read_session_data(spark)
    df.show()
    songs_per_user = create_users_and_distinct_songs_count(df)
    logger.info(f"Sample distinct songs per user: \n '{songs_per_user[:5]}'")
    popular_songs = create_popular_songs(df)
    logger.info(
        f"First five of top 100 popular songs:\n '{popular_songs[:5]}'"
    )
    df_sessions = longest_sessions_with_tracklist(df)
    logger.info("First five of top 100 popular songs:\n")
    df_sessions.show()
