import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()

cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS core.raw_amzn_books_data AS 
        SELECT * FROM read_csv_auto(
            'book_reviews/core/raw_amzn_books_data.csv'
        );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS core.raw_amzn_books_rating AS 
        SELECT * FROM read_csv_auto(
            'book_reviews/core/raw_amzn_books_rating.csv'
        );
""")

#print(cursor.execute('DESCRIBE core.raw_amzn_books_data;').fetchall())
#print(cursor.execute('DESCRIBE core.raw_amzn_books_rating;').fetchall())

tables = cursor.execute('PRAGMA show_tables;').fetchall()

counts = cursor.execute("""
    SELECT COUNT(*) FROM core.raw_amzn_books_data
    UNION
    SELECT COUNT(*) FROM core.raw_amzn_books_rating;
""").fetchall()

for k, v in zip(tables, counts):
    print(k, v)

cursor.close()
conn.close()