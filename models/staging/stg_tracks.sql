with 
source as (
    select * from {{ source('spotify', 'tracks') }}
),
renamed as (
    select
        id,
        name,
        popularity,
        duration_ms,
        explicit,
        artists,
        id_artists,
        release_date,
        danceability,
        energy,
        key,
        loudness,
        mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        time_signature
    from source

)
select * from renamed