
with base as (
  select
    cast(followers as int64) as followers,
    json_extract_array(replace(genres, "'", '"')) as genres_array
  from {{ ref("stg_spotify__genres") }}
),

exploded as (
  select
    replace(genre, '"', '') as genre,
    followers / array_length(genres_array) as weighted_followers
  from base,
  unnest(genres_array) as genre
)

select
  genre,
  sum(weighted_followers) as weighted_total_followers
from exploded
group by genre
order by weighted_total_followers DESC