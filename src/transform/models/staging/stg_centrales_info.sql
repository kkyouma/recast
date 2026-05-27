with
    src as (select * from {{ source("raw", "centrales_info") }}),

    renamed as (
        select
            id_central,
            central as nombre_central,
            region,
            comuna,
            provincia,
            tipo_tecnologia,
            coordenada_este as coordenada_este_utm,
            coordenada_norte as coordenada_norte_utm,
            zona_huso,
            _extracted_at
        from src
    ),

    casted as (
        select
            -- ids
            safe_cast(id_central as int64) as id_central,

            trim(regexp_replace(nombre_central, r'\[[^\]]+\]', '')) as nombre_central,
            coalesce(
                lower(regexp_extract(nombre_central, r'\[([^\]]+)\]')), 'normal'
            ) as state_central,

            region,
            comuna,
            provincia,
            tipo_tecnologia,

            -- numerics
            safe_cast(
                replace(cast(coordenada_este_utm as string), ',', '.') as float64
            ) as coordenada_este_utm,
            safe_cast(
                replace(cast(coordenada_norte_utm as string), ',', '.') as float64
            ) as coordenada_norte_utm,
            safe_cast(regexp_extract(zona_huso, r'^(\d+)') as int64) as zona_huso,

            -- metadata
            _extracted_at,
            current_timestamp() as _loaded_at
        from renamed
    )

select *
from casted
