with

    source as (select * from {{ source("spotify", "artist") }}),

    renamed as (select id, followers, genres, name, popularity from source)

select *
from renamed
