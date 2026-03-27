with 

source as (

    select * from {{ source('spotify', 'artist_genres') }}

),

renamed as (

    select
        id as artist_id,
        followers,
        genres,
        name,
        popularity

    from source

)

select * from renamed