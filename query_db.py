import os
import duckdb

# establish a connection
home_path = os.environ['HOME']
data_path = os.path.join(home_path, 'Documents', 'GitHub', 'BookReviews', 'data')
conn = duckdb.connect(os.path.join(data_path, 'books.duckdb'))
cursor = conn.cursor()

# print out columns in a table
def print_columns(table):
    text = f"PRAGMA table_info('core.{table}');"
    query = cursor.execute(text)
    res = query.fetchall()
    print('Columns:')
    for ix, col in enumerate(res):
        [print((ix+1, val)) for ct, val in enumerate(col) if ct == 1]

# print out table row counts
def print_row_count(table):
    text = f"SELECT COUNT(*) FROM core.{table};"
    query = cursor.execute(text)
    res = query.fetchall()
    print(f'Row count: {res[0]}')

# print out tables in a database, and their columns and counts
query = cursor.execute("PRAGMA show_tables;")
res = query.fetchall()
for k, v in enumerate(res):
    # skip views
    if v[0][:2] == 'vw':
        pass
    else:
        print(f'\nTable {k+1}: {v[0]}')
        print_row_count(v[0])
        print_columns(v[0])

# titles are unique in the raw table (count = count(distinct)) but not in the staged table
# 209456 titles in lowercase, as opposed to 212403 in mixed cases (raw table)

# q = cursor.execute("""
#     SELECT COUNT(DISTINCT title*)
#     FROM core.stg_amzn_books_data;
# """).fetchall()

# print(f'Staged table unique title count: {q}')

# why are there multiple entries for the same title?
# ideally, "books_data" should be unique by title
# q = cursor.execute("""
#     WITH dupe_titles AS (
#         SELECT title
#         FROM core.stg_amzn_books_data
#         GROUP BY title
#             HAVING COUNT(*) > 1
#     )
#     SELECT *
#     FROM core.stg_amzn_books_data
#     WHERE title IN (SELECT title FROM dupe_titles)
#     ORDER BY title;
# """)

# Save CSV for manual analysis
# import pandas as pd
# df = pd.DataFrame(q.fetchall())
# df.to_csv('./data/dupe_titles.csv', index=False)
# print(f'num rows: {len(df)}')

# ISSUES:
# empty rows (after title)
# if same author: different description language, different publication (no ISBN)
# if diff author: possible adaptation of same book OR another book with the same name
# SOLUTIONS:
# dump those with no ratings? Generally they're the ones with less data and/or older editions
# one problem is when none of them have ratings, ex. anne of avonlea
# does this title exist in the ratings table? Yes: with 126 ratings.
# MORE ISSUES:
# so num ratings in books_data != actual num ratings
# cannot rely on num ratings in books_data
# RESULT:
# too many data cleanup issues?
# maybe just join on what joins and call it good enough
# BUT: how do I know I'm joining the deduped title in books_data to the intended same-title
# in books_ratings without also removing the dupes in books_ratings?
# Could ALSO remove all the dupes from the books_ratings...
# AT THE END... will never know if data is 100% "clean" without manual inspection
# so just best effort after all, know when to stop, know the goal
# ISBNs would be better, tying ISBN to a title... maybe use other datasets with that

# q = cursor.execute("""
#     SELECT COUNT(*) FROM core.stg_amzn_books_rating
#     WHERE title = 'anne of avonlea';
# """).fetchall()

# print(q)

# 1967 duplicates

# q = cursor.execute("""
#     SELECT
#         'distinct' AS type
#         , COUNT(*)
#     FROM (SELECT DISTINCT * FROM core.stg_amzn_books_data)
#     UNION
#     SELECT 'count' AS type
#         , COUNT(*)
#     FROM core.stg_amzn_books_data;
# """)fetchall()

# print(q)

# close connection
cursor.close()
conn.close()