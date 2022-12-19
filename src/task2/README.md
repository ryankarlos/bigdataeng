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