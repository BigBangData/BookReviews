import enum
import duckdb
conn = duckdb.connect('./data/books.duckdb')

cursor = conn.cursor()
# pragma = cursor.execute("""
#     PRAGMA show_tables;
#     PRAGMA table_info('core.vw_amzn_books_ratings');
# """
# )

# res1 = pragma.fetchall()

# print('Columns:')
# for ix, col in enumerate(res1):
#     print([(ix+1, val) for ct, val in enumerate(col) if ct == 1])

# query = cursor.execute("""
#     SELECT * lower = 2703963, 2667782
#     2991226 distinct
#     FROM core.stg_amzn_books_rating
#     LIMIT 5;
# """)

# res2 = query.fetchall()
# print(res2)

# q = cursor.execute("""
#     SELECT COUNT(*)
#     FROM core.vw_amzn_books_ratings;
# """)

# print(f'View count: {q.fetchall()}')

q = cursor.execute("""
    SELECT COUNT(*)
    FROM core.stg_amzn_books_rating;
""")

print(f'Staged ratings table count: {q.fetchall()}')

# q2 = cursor.execute("""
#     SELECT COUNT(*) AS CTD
#     FROM (
#         SELECT DISTINCT * FROM core.vw_amzn_books_ratings
#     );
# """)
# print(q2.fetchall())

# for ix, row in enumerate(res2):
#     print('Row', ix+1,': ',
#         [val[:15] for ct, val in enumerate(row) if val]
#     )

cursor.close()
conn.close()