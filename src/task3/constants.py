from pyspark.sql.types import IntegerType, StringType, StructField, StructType

SESSION_PATH = "lastfm-dataset-1K/user-session-track.tsv"
PROFILE_PATH = "lastfm-dataset-1K/user-profile.tsv"

SESSION_SCHEMA = StructType(
    [
        StructField("userid", StringType(), False),
        StructField("timestamp", StringType(), True),
        StructField("artistid", StringType(), True),
        StructField("artistname", StringType(), True),
        StructField("trackid", StringType(), True),
        StructField("trackname", StringType(), True),
    ]
)


PROFILE_SCHEMA = StructType(
    [
        StructField("#id", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("registered", StringType(), True),
    ]
)
