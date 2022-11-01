import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE TABLE IF NOT EXISTS core.raw_amzn_books_data(
        title VARCHAR
    );
"""
)

print(cursor.execute('DESCRIBE core.raw_amzn_books_data;').fetchall())

cursor.close()
conn.close()