from typing import List, Optional

from constants import LOG, SESSION_PATH, SESSION_SCHEMA
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_session_data(spark: SparkSession) -> DataFrame:
    """
    Read session and user profile data and join on userid.
    :param spark: SparkSession
    :return: Spark DataFrame
    """
    LOG.info("Reading data from %s", SESSION_PATH)
    data = (
        spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(SESSION_SCHEMA)
        .load(SESSION_PATH)
    )
    cols_to_drop = ("artistid", "trackid")
    return data.drop(*cols_to_drop).cache()


def write_to_parquet(
    df: DataFrame, path: str, partitions: Optional[List] = None
) -> None:
    """
    persit datfraem to parquet format. Optionally, can persist with partitons, if
    list of columns to parition on is speciifed.
    :param df: Dataframe to write
    :param path: path to write to.
    :param partitions: List of columns to parition on.
    :return: None
    """
    if partitions is None:
        df.write.mode("overwrite").parquet(path)
    else:
        df.write.partitionBy(*partitions).mode("overwrite").parquet(path)
