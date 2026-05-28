{{ config(severity = 'warn') }}

-- Tests that the calculated latitudes and longitudes fall within a reasonable 
-- bounding box for Chile, or are null (nulls are allowed by this test, handled by others if needed).
-- Latitude roughly between -60 and -17
-- Longitude roughly between -115 and -65 (including Easter Island)

select
    id_central,
    latitude,
    longitude
from {{ ref('int_centrales_geo') }}
where
    latitude is not null 
    and longitude is not null
    and (
        latitude > -15.0 or latitude < -65.0
        or longitude > -60.0 or longitude < -120.0
    )
