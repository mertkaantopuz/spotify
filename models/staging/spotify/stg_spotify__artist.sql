with 
source as (
    select * from {{ source('spotify', 'artist') }}
),


renamed as (
    select
        id as artist_id,
        name as artist_name,
        cast(followers as int64) as followers, -- Float gelmiş, tam sayıya çevirmek daha mantıklı
        genres, 
        popularity as artist_popularity
    from source
)

select * from renamed