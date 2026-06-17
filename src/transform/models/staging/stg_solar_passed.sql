with src as (select * from {{ source("raw", "era5_solar") }})

select

    valid_time,
    latitude,
    longitude,

    t2m,
    ssrd,
    tcc,
    fdir,

    -- metadata
    _extracted_at,
    current_timestamp() as _loaded_at

from src
