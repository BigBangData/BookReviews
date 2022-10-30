
/*
Transform csv data
*/

{{ config(materialized='table') }}

with amzn_books_data as (
    select
        Title as title
        , authors
    from {{ source('amzn_books_data') }}
    limit 1000
)

select *
from amzn_books_data;
