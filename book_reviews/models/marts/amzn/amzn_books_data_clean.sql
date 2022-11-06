{{ config(
    materialized="table",
    tags="clean"
) }}

WITH amzn_books_data AS (
    SELECT * FROM {{ ref('stg_amzn_books_data') }}
)

-- remove  rows which are null after the title
, non_null_subset AS (
    SELECT
        title
        , description
        , authors
        , publisher
        , published_date
        , categories
    FROM amzn_books_data
    WHERE 1 = 1
        AND COALESCE(
            title
            , description
            , authors
            , publisher
            , published_date
            , categories
        ) IS NOT NULL
)

-- identify duplicated titles to be removed
, dupe_titles AS (
    SELECT title
    FROM non_null_subset
    GROUP BY title
    HAVING COUNT(*) > 1
)

-- deduplicate and add a table PK
, deduplicated AS (
    SELECT
        title
        , description
        , authors
        , publisher
        , published_date
        , categories
        , ROW_NUMBER() OVER () AS book_id
    FROM non_null_subset
    WHERE 1 = 1
        AND title NOT IN (
            SELECT title FROM dupe_titles
        )
)

SELECT
    book_id
    , title
    , description
    , authors
    , publisher
    , published_date
    , categories
FROM deduplicated
