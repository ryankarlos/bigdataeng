The `generate_data.py` script in the task1 folder generates simulated data for a fictional game, showing players
and the level that they’re currently playing:
Find how many players of each rank are playing each level

We can write a query as below which would join the `Players` and `Levels` tables on `Level_ID` and
then group by Rank and LevelName and then compute ana aggregation (count) which would give the players
of each ran, playing each level.

```SQL
SELECT LevelName, Rank, count(*) as num_players
FROM Players
JOIN Levels ON (Players.Level_ID = Levels.Level_ID)
GROUP BY Rank,LevelName;
```