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

# titles are unique in the raw table (count = count(distinct)) but not in the staged table
# 209456 titles in lowercase, as opposed to 212403 in mixed cases in raw
# q = cursor.execute("""
#     SELECT COUNT(DISTINCT title*)
#     FROM core.stg_amzn_books_data;
# """)
# print(f'Staged table unique title count: {q.fetchall()}')

# begs the question: why are there multiple entries for the same title?
# shouldn't the "books_data" table be unique by title?
q = cursor.execute("""
    WITH dupe_titles AS (
        SELECT title
        FROM core.stg_amzn_books_data
        GROUP BY title
            HAVING COUNT(*) > 1
    )
    SELECT *
    FROM core.stg_amzn_books_data
    WHERE title IN (SELECT title FROM dupe_titles)
    ORDER BY title;
""")

import pandas as pd

df = pd.DataFrame(q.fetchall())
df.to_csv('./data/dupe_titles.csv', index=False)

print(f'num rows: {len(df)}')

# ISSUES:
# empty rows (after title)
# if same author: different description language, different publication (no ISBN)
# if diff author: possible adaptation of same book OR another book with the same name
# IDEA:
# dump those with no ratings? Generally they're the ones with less data and/or older editions
# one problem is when none of them have ratings, ex. anne of avonlea
# does this title exist in the ratings table?
# YES: 126 RATINGS!
q = cursor.execute("""
    SELECT COUNT(*) FROM core.stg_amzn_books_rating
    WHERE title = 'anne of avonlea';
""").fetchall()

print(q)


# 1967 duplicates
q2 = cursor.execute("""
    SELECT
        'distinct' AS type
        , COUNT(*)
    FROM (SELECT DISTINCT * FROM core.stg_amzn_books_data)
    UNION
    SELECT 'count' AS type
        , COUNT(*)
    FROM core.stg_amzn_books_data;
""")

print(q2.fetchall())

# for ix, row in enumerate(res2):
#     print('Row', ix+1,': ',
#         [val[:15] for ct, val in enumerate(row) if val]
#     )

# staged_ratings - view, both inner joins => 2991226 - 2634371 = 356855
#                                                      2634579

cursor.close()
conn.close()