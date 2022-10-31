{{ config(
    enabled=true
) }}

with renamed as (
    select 
        Title as title
        , description as summary
        , authors as author
        , publisher as publisher
        , publishedDate as published_date
        , categories as categories
        , ratingsCount as ratings_count
    from {{ ref('raw_amzn_books_data') }}

)

select * from renamed;
