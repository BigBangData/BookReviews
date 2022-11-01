import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
""")

cursor.execute("""
    CREATE OR REPLACE TABLE core.raw_amzn_books_data AS 
        SELECT * FROM read_csv_auto(
            'book_reviews/core/raw_amzn_books_data.csv'
        );
""")

#print(cursor.execute('DESCRIBE core.raw_amzn_books_data;').fetchall())

cursor.close()
conn.close()