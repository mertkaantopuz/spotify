with 

source as (

    select * from {{ source('spotify', 'genres') }}

),

renamed as (

    select
        followers,
        genres
    from source

)

select * from renamed