{{
    config(
        partition_by={"field": "fecha", "data_type": "date"},
        cluster_by=["tipo_tecnologia", "clasificacion_fuente"],
    )
}}

with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where state_central = 'normal'
    ),

    -- Capacidad instalada: MAX(potencia_maxima_mw) por central por día,
    -- luego sumamos por tecnología para obtener la capacidad total
    capacidad_por_tech as (
        select fecha, tipo_tecnologia, sum(max_potencia) as capacidad_instalada_mw
        from
            (
                select
                    fecha,
                    tipo_tecnologia,
                    id_central,
                    max(potencia_maxima_mw) as max_potencia
                from source
                group by fecha, tipo_tecnologia, id_central
            )
        group by fecha, tipo_tecnologia
    ),

    daily_by_tech as (
        select
            s.fecha,
            s.tipo_tecnologia,
            s.clasificacion_fuente,

            -- Generación (MW durante 1h = MWh)
            sum(s.generacion_real_mw) as generacion_total_mwh,
            avg(s.generacion_real_mw) as generacion_promedio_mw,
            max(s.generacion_real_mw) as generacion_maxima_mw,

            -- Operación
            countif(s.generacion_real_mw > 0) as horas_con_generacion,
            count(
                distinct case when s.generacion_real_mw > 0 then s.id_central end
            ) as num_centrales_activas,

            -- Capacidad (desde CTE)
            c.capacidad_instalada_mw
        from source as s
        inner join
            capacidad_por_tech as c
            on s.fecha = c.fecha
            and s.tipo_tecnologia = c.tipo_tecnologia
        group by
            s.fecha, s.tipo_tecnologia, s.clasificacion_fuente, c.capacidad_instalada_mw
    ),

    final as (
        select
            fecha,
            tipo_tecnologia,
            clasificacion_fuente,
            generacion_total_mwh,
            round(generacion_promedio_mw, 2) as generacion_promedio_mw,
            generacion_maxima_mw,
            horas_con_generacion,
            num_centrales_activas,
            capacidad_instalada_mw,
            round(
                safe_divide(generacion_total_mwh, capacidad_instalada_mw * 24), 4
            ) as factor_planta
        from daily_by_tech
    )

select *
from final
