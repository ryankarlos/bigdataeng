# Big Data Engineering Tasks

## Task1 

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

## Task2

`parse_text_file.sh` in task2 folder contains a complex bash command. What can you tell about the expected contents 
of the input file? What does the command do, and how would you simplify it?

The contents of the input file should be similar to `inputfile.txt` in task2 folder. This is a tab delimited file
with n number of columns, where the 2nd column contains urls for downloading text files. To understand why it is likely
to be structured in this way, lets break down each line of the bash command and see what it is trying to do:

```bash
for a in `yes | nl | head -50 | cut -f 1`; do 
```

This will run a for loop 50 times, where `a` is assigned the value in a sequence fro 1 to 50.

```bash
  head -$(($a*2)) inputfile | tail -1 | 
```
This will pipe every alternate row in the text file (skipping the first row - column and whitespace in between).
In this query, this is achieved by taking the final row of the first `n` rows of the text file, where `n=a*2`.
The output is piped to the next command below

```bash
awk 'BEGIN{FS="\t"}{print $2}' | xargs wget -c 2> /dev/null;
```

`Awk` reads and parses each line from input based on whitespace character by default and set the variables 
$1,$2 and etc. the `FS` variable is used to set the field separator for each record, which is set to tab delimiter `\t`
In this case, we have each input row, which is tab separated, hence `awk 'BEGIN{FS="\t"}{print $2}'` will parse each value
separated by whitespace and assign it to $1 , $2 etc in order, and prints out the value of $2 which would be the url.
This is then piped to `xargs wget -c` which would try downloading each file from the url using `wget` command. If there is
an error, this would be redirected to /dev/null and not stream to stdout.

## Task3

Go to the following URL, and download the dataset on sampled Last.fm usage: http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html
Using pyspark perform the following tasks:

* Create a list of user IDs, along with the number of distinct songs each user has played.

* Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.

Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user, where 
each song is started within 20 minutes of the previous song’s start time. Create a list of the top 10 longest sessions
(by elapsed time), with the following information about each session: userid, timestamp of first and last songs in the 
session, and the list of songs played in the session (in order of play).
