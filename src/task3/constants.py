from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

SESSION_PATH = "lastfm-dataset-1K/user-session-track.tsv"

SESSION_SCHEMA = StructType(
    [
        StructField("userid", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("artistid", StringType(), True),
        StructField("artistname", StringType(), True),
        StructField("trackid", StringType(), True),
        StructField("trackname", StringType(), True),
    ]
)

