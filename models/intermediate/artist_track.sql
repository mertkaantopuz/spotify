{{ config(
    materialized='table'
) }}

with cleaned as (
    select
        track_id,

        -- artist id
        replace(artist_ids_list, "'", "") as artist_str,

        -- artist names temiz string
        regexp_replace(artist_names_list, r"^\[|\]$", "") as artist_names_clean,

        track_name,
        track_popularity,
        duration_ms,
        duration_sec,
        is_explicit,
        artist_names_list,
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
        
    from {{ ref('stg_spotify__tracks') }}
),

split_artists as (
    select
        track_id,
        trim(artist_id) as artist_id,

        track_name,
        track_popularity,
        duration_ms,
        duration_sec,
        is_explicit,
        artist_names_list,
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
        time_signature,

        -- 🔥 GROUP LOGIC (STRING SPLIT)
        case 
            when array_length(
                split(artist_names_clean, ",")
            ) > 1 then true
            else false
        end as is_group

    from cleaned,

    unnest(
        split(
            regexp_replace(artist_str, r"^\[|\]$", ""),
            ","
        )
    ) as artist_id
)

select 
*,
concat(track_id, "_", artist_id) as primary_key
from split_artists
where artist_id is not null
  and artist_id != ''