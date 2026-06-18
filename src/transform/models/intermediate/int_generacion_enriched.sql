with
    generacion as (select * from {{ ref("stg_generacion_real") }}),

    centrales as (select * from {{ ref("int_centrales_geo") }}),

    joined as (
        select
            -- ids
            g.id_central,

            -- central info
            c.nombre_central,
            g.tipo_tecnologia,
            {{ clasificar_fuente("c.tipo_tecnologia") }} as clasificacion_fuente,
            c.state_central,
            c.region,
            c.comuna,
            c.provincia,

            -- geo
            c.latitude,
            c.longitude,
            c.geo_point,

            -- generation metrics
            g.generacion_real_mw,
            g.potencia_maxima_mw,
            round(
                safe_divide(generacion_real_mw, potencia_maxima_mw), 6
            ) as factor_planta,

            -- timestamps
            g.timestamp_local,
            g.timestamp_utc,
            date(g.timestamp_local) as fecha,
            extract(hour from g.timestamp_local) as hora,
            extract(dayofweek from g.timestamp_local) as dia_semana,
            extract(month from g.timestamp_local) as mes,
            extract(year from g.timestamp_local) as anio,

            -- metadata
            g._extracted_at,
            g._loaded_at
        from generacion as g
        inner join centrales as c on g.id_central = c.id_central
    )

select *
from joined
