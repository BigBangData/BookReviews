WITH renamed AS (
    SELECT
        r.Title AS title,
        r.description AS summary,
        r.authors AS author,
        r.publisher AS publisher,
        r.publishedDate AS published_date,
        r.categories AS categories,
        r.ratingsCount AS ratings_count
    FROM {{ source('data', 'raw_amzn_books_data') }} AS r
)

SELECT
    *
FROM renamed;
