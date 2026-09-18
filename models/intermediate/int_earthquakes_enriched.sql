select
    earthquake_id,
    magnitude,
    place,
    event_time,
    longitude,
    latitude,
    depth_km,
    case
        when magnitude < 4 then 'minor'
        when magnitude < 5 then 'light'
        when magnitude < 6 then 'moderate'
        when magnitude < 7 then 'strong'
        else 'major'
    end as band,
    case
        when position(',', place) = 0 then place
        else trim(substring(place, position(',', place)+2)) 
    end as region,
    extract(year from event_time) as year,
    extract(month from event_time) as month

from {{ ref('stg_earthquakes') }}