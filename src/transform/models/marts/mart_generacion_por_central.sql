{{
    config(
        partition_by={"field": "fecha", "data_type": "date"},
        cluster_by=["id_central", "region"],
    )
}}

with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where state_central = 'normal'
    ),

    daily_by_central as (
        select
            fecha,
            id_central,
            any_value(nombre_central) as nombre_central,
            any_value(tipo_tecnologia) as tipo_tecnologia,
            any_value(clasificacion_fuente) as clasificacion_fuente,
            any_value(region) as region,
            any_value(comuna) as comuna,
            any_value(latitude) as latitude,
            any_value(longitude) as longitude,

            -- Generación (MW durante 1h = MWh)
            sum(generacion_real_mw) as generacion_total_mwh,
            round(avg(generacion_real_mw), 2) as generacion_promedio_mw,
            max(generacion_real_mw) as generacion_maxima_mw,
            min(generacion_real_mw) as generacion_minima_mw,

            -- Capacidad
            max(potencia_maxima_mw) as potencia_maxima_mw,

            -- Operación
            countif(generacion_real_mw > 0) as horas_operacion,
            count(*) as horas_con_datos
        from source
        group by fecha, id_central
    ),

    final as (
        select
            fecha,
            id_central,
            nombre_central,
            tipo_tecnologia,
            clasificacion_fuente,
            region,
            comuna,
            latitude,
            longitude,
            generacion_total_mwh,
            generacion_promedio_mw,
            generacion_maxima_mw,
            generacion_minima_mw,
            potencia_maxima_mw,
            round(
                safe_divide(generacion_total_mwh, potencia_maxima_mw * 24), 4
            ) as factor_planta,
            horas_operacion,
            horas_con_datos
        from daily_by_central
    )

select *
from final
