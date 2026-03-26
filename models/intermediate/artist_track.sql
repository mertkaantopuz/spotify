-- models/flatten_tracks_artists.sql
{{ config(
    materialized='table'
) }}

with cleaned as (
    select
        track_id,
        -- sadece tek tırnakları kaldır
        replace(artist_ids_list, "'", "") as artist_str,
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
        trim(artist_id) as artist_id,track_name,
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
    from cleaned,
    -- köşeli parantezleri temizle, virgülle ayır
    unnest(split(regexp_replace(artist_str, r"^\[|\]$", ""), ",")) as artist_id
)

select *,
Concat(track_id,"_",artist_id) as primary_key
from split_artists
where artist_id is not null
  and artist_id != ''