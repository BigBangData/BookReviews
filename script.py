import duckdb

cursor = duckdb.connect()

# see https://duckdb.org/docs/sql/data_types/overview
data_cols = "{'title': 'TEXT', 'description': 'TEXT', 'authors': 'TEXT', 'image': 'TEXT', \
    'preview_link': 'TEXT', 'publisher': 'TEXT', 'published_date': 'TEXT', \
    'info_link': 'TEXT', 'categories': 'TEXT', 'ratings_count': 'TEXT'}"

rating_cols = "{'id': 'TEXT', 'title': 'TEXT', 'price': 'TEXT', 'user_id': 'TEXT', \
    'profile_name': 'TEXT', 'review_helpfuless': 'TEXT', 'review_score': 'INTEGER', \
    'review_time': 'INTEGER', 'review_summary': 'TEXT', 'review_text': 'TEXT'}"

books_data = f"read_csv_auto('./book_reviews/seeds/amzn_books_data.csv', \
    delim=',', header=True, columns={data_cols})"

books_rating = f"read_csv_auto('./book_reviews/seeds/amzn_books_rating.csv', \
    delim=',', header=True, columns={rating_cols})"

amzn_books_data = f"""
    SELECT
        title
        , authors
        , publisher
        , published_date
        , ratings_count
    FROM {books_data}
    WHERE 1 = 1
        AND authors IS NOT NULL
        AND publisher IS NOT NULL
        AND published_date IS NOT NULL
    ORDER BY
        publisher
        , authors
        , title
    LIMIT 10;
"""

print(cursor.execute(amzn_books_data).fetch_df())