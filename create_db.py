import duckdb
conn = duckdb.connect('./database/books.duckdb')

cursor = conn.cursor()
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
"""
)
 
#cursor.execute("COPY sales FROM 'sales.csv' (HEADER)")
#print(cursor.execute('select count(*) from sales').fetchall())
cursor.close()
conn.close()