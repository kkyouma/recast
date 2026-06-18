with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where tipo_tecnologia = 'Solar'
    ),

    era5_lookup as (
        select id_central, lat_era5, lon_era5 from {{ ref("int_compatibilidad_era5") }}
    ),

    filled as (
        select
            g.id_central,
            g.timestamp_utc,
            g.fecha,
            l.lat_era5,
            l.lon_era5,
            g.generacion_real_mw,
            g.potencia_maxima_mw,
            g.factor_planta
        from source as g
        inner join era5_lookup as l on g.id_central = l.id_central
    )

select *
from filled
