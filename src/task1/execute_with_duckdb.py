import duckdb

from .queries import query1, query2, query4

cursor = duckdb.connect()

cursor.execute(query1)
cursor.execute(query2)
df = cursor.execute(query4).df()
print(df)
