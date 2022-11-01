{{ config(
    materialized="table",
    tags="staging"
) }}

WITH renamed AS (
    SELECT
        r.Title AS title
    FROM {{ source('core', 'raw_amzn_books_data') }} AS r
)

SELECT * FROM renamed
