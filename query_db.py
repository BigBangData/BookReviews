import enum
import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()
pragma = cursor.execute("""
    PRAGMA table_info('core.stg_amzn_books_rating');
"""
)

res1 = pragma.fetchall()

print('Columns:')
for ix, col in enumerate(res1):
    print([(ix+1, val) for ct, val in enumerate(col) if ct == 1])

# query = cursor.execute("""
#     SELECT *
#     FROM core.stg_amzn_books_rating
#     LIMIT 5;
# """)

# res2 = query.fetchall()
# print(res2)

print(cursor.execute('SELECT COUNT(*) FROM core.stg_amzn_books_rating;').fetchall())
# for ix, row in enumerate(res2):
#     print('Row', ix+1,': ',
#         [val[:15] for ct, val in enumerate(row) if val]
#     )

cursor.close()
conn.close()