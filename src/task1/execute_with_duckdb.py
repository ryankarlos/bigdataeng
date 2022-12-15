import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

from src.task1.queries import LEVELS_INSERT, PLAYERS_INSERT, QUERY


def main():
    cursor = duckdb.connect()
    cursor.execute(PLAYERS_INSERT)
    cursor.execute(LEVELS_INSERT)
    df = cursor.execute(QUERY).df()
    expected = pd.DataFrame(
        [
            ("Cadet", "Caverns of Doom", 2),
            ("Lieutenant", "Lake of the Undead", 1),
            ("Lieutenant", "Forest of Evil Things", 1),
            ("Colonel", "Island of Darkness", 1),
        ],
        columns=["Rank", "Level_Name", "num_players"],
    )
    assert_frame_equal(df, expected)
    return df


if __name__ == "__main__":
    df = main()
    print(df)
