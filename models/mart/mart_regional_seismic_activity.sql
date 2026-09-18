select
    region,
    year,
    count(earthquake_id) as event_count,
    round(avg(magnitude), 2) as avg_magnitude
from {{ ref('int_earthquakes_enriched') }}
group by region, year
order by event_count desc