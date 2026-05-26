with
    src as (
        select *
        from {{ source("raw", "generacion_real") }}
        where fecha_hora < current_timestamp()
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
            fecha_hora as timestamp_local

        from src
    ),

    casted as (
        select
            id_central,
            tipo_tecnologia,
            generacion_real_mw,
            potencia_maxima_mw,
            timestamp_local
        from renamed
    )

select *
from casted
