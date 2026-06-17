with
    source as (select * from {{ ref("stg_solar_passed") }}),

    features as (
        select
            valid_time as timestamp_utc,
            latitude as lat_era5,
            longitude as lon_era5,

            -- Conversión de unidades
            round(t2m - 273.15, 2) as temperatura_celsius,
            round(ssrd / 3600.0, 2) as ghi_wm2,
            round(fdir / 3600.0, 2) as dni_wm2,
            round(tcc * 100.0, 1) as nubosidad_pct,
            round(safe_divide(fdir, nullif(ssrd, 0)), 4) as fraccion_directa,

            -- Cyclical encoding (acos(-1) = π en BigQuery)
            round(
                cos(2 * acos(-1) * extract(hour from valid_time) / 24), 6
            ) as hora_cos,
            round(
                sin(2 * acos(-1) * extract(hour from valid_time) / 24), 6
            ) as hora_sin,
            round(
                cos(2 * acos(-1) * extract(month from valid_time) / 12), 6
            ) as mes_cos,
            round(
                sin(2 * acos(-1) * extract(month from valid_time) / 12), 6
            ) as mes_sin,

            -- Originales (para EDA, no van al modelo)
            extract(hour from valid_time) as hora_utc,
            extract(month from valid_time) as mes

        from source
    )

select *
from features
