{{ config(
    materialized="view",
    tags="amzn"
) }}

WITH books AS (
    SELECT * FROM {{ ref('stg_amzn_books_data') }}
)

, ratings AS (
    SELECT * FROM {{ ref('stg_amzn_books_rating') }}
)

, final AS (
    SELECT
        b.title AS data_title
        , r.title AS rating_title
        , b.description
        , b.authors
        , b.publisher
        , b.published_date
        , b.categories
        , b.ratings_count
        , r.price
        , r.user_id
        -- , r.profile_name
        -- , r.review_helpfulness
        -- , r.review_score
        -- , r.review_time
        -- , r.review_summary
        -- , r.review_text
    FROM books AS b
        FULL OUTER JOIN ratings AS r
            ON b.title = r.title
    WHERE 1 = 1
        AND b.title IS NULL
            OR r.title IS NULL
)

SELECT * FROM final
