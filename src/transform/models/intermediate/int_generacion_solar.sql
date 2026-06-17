with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where state_central = 'normal' and tipo_tecnologia = 'Solar'
    ),

    era5_lookup as (
        select id_central, lat_era5, lon_era5 from {{ ref("int_compatibilidad_era5") }}
    ),

    -- Forward-fill potencia_maxima_mw con el último valor conocido
    final as (
        select
            *,
            round(
                safe_divide(generacion_real_mw, potencia_maxima_mw), 6
            ) as factor_planta
        from filled
    )

select *
from final
