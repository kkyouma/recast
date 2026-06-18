with
    src as (
        select *
        from {{ source("raw", "generacion_real") }}
        -- Required by BigQuery for partition pruning
        -- Don't filter anything, just operational. Will change in future for
        -- incremental
        where fecha_hora < current_datetime()
    ),

    renamed as (
        select
            -- ids
            id_central,

            -- strings
            tipo_tecnologia,

            -- numerics
            gen_real_mw as generacion_real_mw,
            potencia_maxima as potencia_maxima_mw,

            -- timestamps
            fecha_hora as timestamp_local,
            _extracted_at
        from src
    ),

    casted as (
        select
            -- ids
            safe_cast(id_central as int64) as id_central,

            -- strings
            safe_cast(tipo_tecnologia as string) as tipo_tecnologia,

            -- numerics
            safe_cast(
                replace(cast(generacion_real_mw as string), ',', '.') as float64
            ) as generacion_real_mw,
            safe_cast(
                replace(cast(potencia_maxima_mw as string), ',', '.') as float64
            ) as potencia_maxima_mw,

            -- timestamps
            timestamp_local,
            timestamp(datetime(timestamp_local), 'America/Santiago') as timestamp_utc,
            _extracted_at,
            current_timestamp() as _loaded_at
        from renamed
        qualify
            row_number() over (
                partition by id_central, timestamp_local order by _extracted_at desc
            )
            = 1
    )

select *
from casted
