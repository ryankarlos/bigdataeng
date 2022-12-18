import logging

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SESSION_PATH = "data/lastfm-dataset-1K/user-session-track.tsv"
RESULTS_PATH = "/tmp/results/task3/"

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

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
