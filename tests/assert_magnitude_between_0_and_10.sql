select *
from {{ ref('int_earthquakes_enriched')}}
where magnitude < 0 or magnitude > 10