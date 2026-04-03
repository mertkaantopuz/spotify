-- models/intermediate/tracks_scaled.sql

{{ config(
    materialized='table'
) }}

with base as (
    select
        track_id,

        cast(duration_sec as float64) as duration_sec,
        cast(danceability as float64) as danceability,
        cast(energy as float64) as energy,
        cast(valence as float64) as valence,
        cast(tempo as float64) as tempo,
        cast(is_explicit as int64) as is_explicit,
        cast(instrumentalness as float64) as instrumentalness,
        cast(liveness as float64) as liveness,
        cast(speechiness as float64) as speechiness,
        cast(acousticness as float64) as acousticness,
        release_date
    from {{ ref('stg_spotify__tracks') }}
    where release_date is not null
      and extract(year from cast(release_date as date)) != 1900
),

stats as (
    select
        min(tempo) as min_tempo,
        max(tempo) as max_tempo
    from base
),

final as (
    select
        b.track_id,

        b.duration_sec,

        -- normalize tempo
        (b.tempo - s.min_tempo) 
        / nullif(s.max_tempo - s.min_tempo, 0) as tempo_norm,

        b.danceability,
        b.energy,
        b.valence,
        b.is_explicit,
        b.instrumentalness,
        b.liveness,
        b.speechiness,
        b.acousticness,

        -- 🔥 sadece yıl
        extract(year from cast(b.release_date as date)) as year

    from base b
    cross join stats s
)

select *
from final