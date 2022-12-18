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
from pyspark.sql.window import Window


def create_users_and_distinct_songs_count(df: DataFrame) -> DataFrame:
    """
    Create a list of user IDs, along with the number of distinct songs each user has played.
    :param df: Spark DataFrame
    :return: DataFrame
    """
    df1 = df.select("userid", "artistname", "trackname").dropDuplicates()
    df2 = (
        df1.groupBy("userid")
        .agg(count("*").alias("DistinctTrackCount"))
        .orderBy(desc("DistinctTrackCount"))
    )
    return df2


def create_popular_songs(df: DataFrame, limit=100) -> DataFrame:
    """
    Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of
    times each was played..
    :param df: Spark DataFrame
    :param limit: int
    :return: DataFrame
    """
    df1 = (
        df.groupBy("artistname", "trackname")
        .agg(count("*").alias("CountPlayed"))
        .orderBy(desc("CountPlayed"))
        .limit(limit)
    )
    return df1


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
