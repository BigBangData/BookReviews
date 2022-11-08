{{ config(
    materialized="view",
    tags="amzn"
) }}

WITH books AS (
    SELECT * FROM {{ ref('amzn_books_data_clean') }}
)

, ratings AS (
    SELECT * FROM {{ ref('stg_amzn_books_rating') }}
)

, final AS (
    SELECT DISTINCT
        b.book_id
        , b.title
        , b.description
        , b.authors
        , b.publisher
        , b.published_date
        , b.categories
        , r.user_id
        , r.review_helpfulness
        , r.review_score
        , r.review_time
        , r.review_summary
        , r.review_text
    FROM ratings AS r
    INNER JOIN books AS b
        ON b.title = r.title
)

SELECT * FROM final
