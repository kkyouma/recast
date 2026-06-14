with
    source as (
        select
            id_central,
            nombre_central,
            tipo_tecnologia,
            {{ clasificar_fuente("tipo_tecnologia") }} as clasificacion_fuente,
            region,
            latitude,
            longitude,
            geo_point
        from {{ ref("int_centrales_geo") }}
        where
            state_central = 'normal' and latitude is not null and longitude is not null
    ),

    calculated as (
        select
            *,
            -- Redondear a la grilla de 0.25 más cercana
            round(latitude * 4) / 4 as lat_era5,
            round(longitude * 4) / 4 as lon_era5
        from source
    ),

    distance as (
        select
            *,
            -- Diferencia absoluta en grados
            abs(latitude - lat_era5) as diferencia_lat,
            abs(longitude - lon_era5) as diferencia_lon,
            -- Distancia exacta en metros usando GEOGRAPHY
            st_distance(
                geo_point, st_geogpoint(lon_era5, lat_era5)
            ) as distancia_era5_metros
        from calculated
    ),

    scored as (
        select
            id_central,
            nombre_central,
            tipo_tecnologia,
            clasificacion_fuente,
            region,
            latitude as lat_real,
            longitude as lon_real,
            lat_era5,
            lon_era5,
            round(diferencia_lat, 4) as diferencia_lat_grados,
            round(diferencia_lon, 4) as diferencia_lon_grados,
            round(distancia_era5_metros, 2) as distancia_era5_metros,
            case
                when distancia_era5_metros < 1000
                then 'Perfecta'
                when distancia_era5_metros >= 1000 and distancia_era5_metros < 5000
                then 'Excelente'
                when distancia_era5_metros >= 5000 and distancia_era5_metros < 10000
                then 'Buena'
                when distancia_era5_metros >= 10000 and distancia_era5_metros < 15000
                then 'Regular'
                else 'Mala'
            end as categoria_precision
        from distance
    )

select *
from scored
