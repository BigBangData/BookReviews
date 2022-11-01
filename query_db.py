import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()
query = cursor.execute("""
    PRAGMA table_info('core.raw_amzn_books_data');
"""
)

res = query.fetchall()

# print out columns
for col in res:
    [print(i) for ct, i in enumerate(col) if ct == 1]

print(cursor.execute('SELECT COUNT(*) FROM core.raw_amzn_books_data;').fetchall())

cursor.close()
conn.close()