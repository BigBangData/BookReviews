import duckdb
conn = duckdb.connect('books.duckdb')

cursor = conn.cursor()
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS amzn;
"""
)
 
#cursor.execute("COPY sales FROM 'sales.csv' (HEADER)")
#print(cursor.execute('select count(*) from sales').fetchall())
cursor.close()
conn.close()