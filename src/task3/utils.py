from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def spark_session_setup(memory: int = 4):
    """
    Setup spark session. Some spark config properties may  need to be configured
    if running locally and depending on machine specs. For example to avoid warnings similar to:
    https://stackoverflow.com/questions/46907447/meaning-of-apache-spark-warning-calling-spill-on-rowbasedkeyvaluebatch
    :param memory: executor and driver memory in GB.
    :return:
    """
    conf = SparkConf()
    conf.set("spark.driver.memory", f"{memory}g")
    conf.set("spark.executor.memory", f"{memory}g")
    spark = (
        SparkSession.builder.appName("lastfm").config(conf=conf).getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
