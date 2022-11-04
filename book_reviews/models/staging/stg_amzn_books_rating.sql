{{ config(
    materialized="table",
    tags="staging"
) }}

WITH renamed AS (
    SELECT
        r.Id AS id
        , LOWER(r.Title) AS title
        , r.Price AS price
        , r.User_id AS user_id
        , r.profileName AS profile_name
        , r."review/helpfulness" AS review_helpfulness
        , r."review/score" AS review_score
        , r."review/time" AS review_time
        , r."review/summary" AS review_summary
        , r."review/text" AS review_text
    FROM {{ source('core', 'raw_amzn_books_rating') }} AS r
    -- deduplicate
    GROUP BY
        id
        , title
        , price
        , user_id
        , profile_name
        , review_helpfulness
        , review_score
        , review_time
        , review_summary
        , review_text
)

SELECT * FROM renamed
