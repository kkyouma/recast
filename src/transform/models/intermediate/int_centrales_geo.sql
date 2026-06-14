with
    source as (select * from {{ ref("stg_centrales_info") }}),

    with_coords as (
        select
            *,
            {{ target.schema }}.utm_to_latlon(
                coordenada_este_utm, coordenada_norte_utm, zona_huso
            ) as coords
        from source
        where
            coordenada_este_utm is not null
            and coordenada_norte_utm is not null
            and zona_huso is not null
    ),

    final as (
        select
            id_central,
            nombre_central,
            state_central,
            region,
            comuna,
            provincia,
            tipo_tecnologia,
            coordenada_este_utm,
            coordenada_norte_utm,
            zona_huso,
            coords.latitude,
            coords.longitude,
            st_geogpoint(coords.longitude, coords.latitude) as geo_point,
            _extracted_at,
            _loaded_at
        from with_coords
        qualify
            row_number() over (partition by id_central order by _extracted_at desc) = 1
    )

select *
from final
