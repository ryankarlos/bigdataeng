import os  # noqa

from src.task3.constants import LOG, RESULTS_PATH  # noqa
from src.task3.io import read_session_data, write_to_parquet  # noqa
from src.task3.transformations import (
    create_popular_songs,
    create_users_and_distinct_songs_count,
    longest_sessions_with_tracklist,
)
from src.task3.utils import spark_session_setup


def main():
    spark = spark_session_setup()
    df = read_session_data(spark)
    songs_per_user = create_users_and_distinct_songs_count(df)
    # write_to_parquet(songs_per_user, os.path.join(RESULTS_PATH, "distinct_songs.parquet"))
    LOG.info(
        f"Sample list of distinct Songs per user: \n {songs_per_user.collect()[:10]}"
    )
    popular_songs = create_popular_songs(df)
    # write_to_parquet(popular_songs, os.path.join(RESULTS_PATH, "popular_songs.parquet"))
    LOG.info(f"List of top 100 popular songs:\n {popular_songs.collect()}")
    df_sessions = longest_sessions_with_tracklist(df)
    # write_to_parquet(df_sessions, os.path.join(RESULTS_PATH, "longest_sessions.parquet"))
    LOG.info("Longest sessions with track list:\n")
    df_sessions.show()


if __name__ == "__main__":
    main()
