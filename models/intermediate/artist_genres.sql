with base as (
  select
    artist_id,
    cast(followers as int64) as followers,
    json_extract_array(replace(genres, "'", '"')) as genres_array,
    cast(popularity as int64) as artist_popularity
  from {{ ref("stg_spotify__artist_genres") }}
),

exploded as (
  select
    artist_id,
    replace(genre, '"', '') as genre,
    followers,
    array_length(genres_array) as genre_count,
    artist_popularity
  from base,
  unnest(genres_array) as genre
)

select
  artist_id,
  genre,
  followers / genre_count as weighted_followers,
  artist_popularity
from exploded
where genre is not null