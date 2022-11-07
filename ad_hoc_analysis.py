import os
import duckdb

# next up:
# recreate database (create_db.py + dbt run)
# check that size is less than 16.7 GB (size now after failed build of table)
# dbt run took 58s
# size 9.6 GB (prob because I renamed into vw_ so --full-refresh didn't replace the badly built table)

# remodel view as table
# repeat steps above, see if table builds, see size of database
# table doesn't build: see unicode error
# size 9.6 GB

# paths
GIT_DIR = os.path.join(os.environ['HOME'], 'Documents', 'GitHub', 'BookReviews')
DATA_DIR = os.path.join(GIT_DIR, 'data')
CORE_DIR = os.path.join(GIT_DIR, 'book_reviews', 'core')

# establish conn
conn = duckdb.connect(os.path.join(DATA_DIR, 'books.duckdb'))
cursor = conn.cursor()

# query
# q = cursor.execute("""
#     SELECT
#         MAX(book_id)
#     FROM core.amzn_books_data_clean
# """)

# print(q.fetchall())

# cleaned up books_data to remove mostly null entries
# print(210437 - 23992)

# reassess duplication of titles

# titles are unique in the raw table (count = count(distinct)) but not in the staged table
# 209456 titles in lowercase, as opposed to 212403 in mixed cases (raw table)

# q = cursor.execute("""
#     SELECT
#         COUNT(DISTINCT title) AS uniq_title_ct
#         , COUNT(*) AS num_rows
#     FROM core.stg_amzn_books_data;
# """)

# print(q.fetchall())

# ideally, "books_data" should be unique by title
# q = cursor.execute("PRAGMA table_info('core.stg_amzn_books_data');")
# cols = []
# for ix, col in enumerate(q.fetchall()):
#     cols.append([val for ct, val in enumerate(col) if ct == 1][0])

# q = cursor.execute("""
#     WITH dupe_titles AS (
#         SELECT title
#         FROM core.stg_amzn_books_data
#         GROUP BY title
#             HAVING COUNT(book_id) > 1
#     )
#     SELECT *
#     FROM core.stg_amzn_books_data
#     WHERE
#         title IN (SELECT title FROM dupe_titles)
#     ORDER BY
#         title
#         , authors
#         , published_date;
# """)

# save csv for manual analysis
# import pandas as pd
# df = pd.DataFrame(q.fetchall(), columns=cols)
# df.to_csv('./data/dupe_titles.csv', index=False)
# print(f'num rows: {len(df)}')

# ISSUES:
# empty rows (after title)
# if same author: different description language, different publication (no ISBN)
# if diff author: possible adaptation of same book OR another book with the same name

# POSSIBLE SOLUTIONS:
# dump those with no ratings? Generally they're the ones with less data and/or older editions
# one problem is when none of them have ratings, ex. `anne of avonlea` below
# does this title exist in the ratings table? Yes: with 126 ratings
# so num ratings in books_data != actual num ratings
# cannot rely on num ratings in books_data --> CREATE a num_ratings after merging with ratings

# RESULT:
# do not clean small subset that would need manual review
# dump them so as not to have to clean ratings as well, then inner join to ratings which dumps the dups

# q = cursor.execute("""
#     SELECT COUNT(*) FROM core.stg_amzn_books_rating
#     WHERE title = 'anne of avonlea';
# """).fetchall()
# print(q)

# q = cursor.execute("""
#     SELECT
#         'distinct' AS type
#         , COUNT(*)
#     FROM (SELECT DISTINCT * FROM core.stg_amzn_books_data)
#     UNION
#     SELECT 'count' AS type
#         , COUNT(*)
#     FROM core.stg_amzn_books_data;
# """).fetchall()

# print(q)


# close conn
cursor.close()
conn.close()
