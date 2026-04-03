-- models/intermediate/artist_genres.sql

{{ config(
    materialized='table'
) }}

with base as (
  select
    artist_id,
    cast(followers as int64) as followers,

    -- JSON fix
    json_extract_array(
      replace(genres, "'", '"')
    ) as genres_array,

    cast(popularity as int64) as artist_popularity
  from {{ ref("stg_spotify__artist_genres") }}
),

exploded as (
  select
    artist_id,

    -- genre temizleme
    replace(genre, '"', '') as genre,

    followers,

    -- kaç genre var
    array_length(genres_array) as genre_count,

    artist_popularity
  from base,
  unnest(genres_array) as genre
)

select
  artist_id,

  -- 🔥 FINAL GENRE FORMAT
  array_to_string(
    array(
      select array_to_string(
        array(
          select initcap(part)
          from unnest(split(word, '-')) as part
        ),
        '-'
      )
      from unnest(
        split(replace(genre, '"', ''), ' ')
      ) as word
    ),
    ' '
  ) as genre,

  followers / genre_count as weighted_followers,
  artist_popularity

from exploded
where genre is not null