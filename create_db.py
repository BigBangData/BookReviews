import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE TABLE IF NOT EXISTS core.raw_amzn_books_data AS 
        SELECT * FROM read_csv_auto('./data/csv/raw_amzn_books_data.csv');
"""
)
 
#cursor.execute("COPY sales FROM 'sales.csv' (HEADER)")
print(cursor.execute('SELECT COUNT(*) FROM core.raw_amzn_books_data;').fetchall())

cursor.close()
conn.close()