import logging
from typing import List

from constants import SESSION_PATH, SESSION_SCHEMA
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count, desc, expr, lag, round, unix_timestamp
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
    return data.drop(*cols_to_drop)


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


def create_top_ten_longest_sessions(df: DataFrame, limit=10, session_cutoff=20) -> List[Row]:
    """
    Creates a list of the top 10 longest sessions (by elapsed time), with the following information about
    each session: userid, timestamp of first and last songs in the  session, and the list of songs played
    in the session (in order of play).
    :param df: Spark DataFrame
    :param limit: int
    :return: List
    """
    windowSpec = Window.partitionBy("userid").orderBy("timestamp")
    df1 = (
        df.withColumn("pretimestamp", lag("timestamp").over(windowSpec))
        .withColumn(
            "delta_mins", round(unix_timestamp("timestamp") - unix_timestamp("pretimestamp")) / 60
        )
        .withColumn("sessionid", expr("CASE WHEN delta_mins > session_cutoff THEN 1 ELSE 0 END"))
    )
    return df1.collect()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("lastfm").config(conf=conf).getOrCreate()
    df = read_session_data(spark)
    df.show()
    songs_per_user = create_users_and_distinct_songs_count(df)
    logger.info(f"Sample distinct songs per user: \n '{songs_per_user[:5]}'")
    popular_songs = create_popular_songs(df)
    logger.info(f"First five of top 100 popular songs:\n '{popular_songs[:5]}'")
