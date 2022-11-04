{{ config(
    materialized="table",
    tags="staging"
) }}

WITH renamed AS (
    SELECT
        LOWER(r.Title) AS title
        , r.description
        , r.authors
        , r.publisher
        , r.publishedDate AS published_date
        , r.categories
        , r.ratingsCount AS ratings_count
    FROM {{ source('core', 'raw_amzn_books_data') }} AS r
)

SELECT * FROM renamed
