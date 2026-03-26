with 
source as (
    select * from {{ source('spotify', 'tracks') }}
),
renamed as (

    select
        id as track_id,
        name as track_name,
        popularity as track_popularity,
        duration_ms,
        round(duration_ms / 1000, 2) as duration_sec,
        cast(explicit as boolean) as is_explicit, -- 0/1 değerini True/False yaptık
        artists as artist_names_list,
        id_artists as artist_ids_list,
        
-- Tarih temizleme: 4 karakterse (yıl), 7 karakterse (yıl-ay) veya tam tarihse ona göre işle
-- örn: 1921 ise --> 1921-01-01
-- örn: 1921-03 ise --> 1921-03-01
        case 
            when length(release_date) = 4 then parse_date('%Y', release_date)
            when length(release_date) = 7 then parse_date('%Y-%m', release_date)
            else safe_cast(release_date as date)
        end as release_date,

-- AUDIO FEATURES
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