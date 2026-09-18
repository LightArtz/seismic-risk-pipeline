select
    id as earthquake_id,
    mag as magnitude,
    place,
    to_timestamp_ntz(time / 1000) as event_time,
    longitude,
    latitude,
    depth_km
from {{ source('raw', 'earthquakes_raw') }}
where mag is not null