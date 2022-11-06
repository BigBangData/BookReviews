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
        b.title
        , b.description
        , b.authors
        , b.publisher
        , b.published_date
        , b.categories
        , b.ratings_count
        , r.price
        , r.user_id
        , r.profile_name
        , r.review_helpfulness
        , r.review_score
        , r.review_time
        , r.review_summary
        , r.review_text
    FROM ratings AS r
    LEFT JOIN books AS b
        ON b.title = r.title
    -- dedupe
    GROUP BY
        b.title
        , b.description
        , b.authors
        , b.publisher
        , b.published_date
        , b.categories
        , b.ratings_count
        , r.price
        , r.user_id
        , r.profile_name
        , r.review_helpfulness
        , r.review_score
        , r.review_time
        , r.review_summary
        , r.review_text
)

SELECT * FROM final
