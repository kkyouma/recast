with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where state_central = 'normal'
    ),

    -- Métricas diarias por central (base para promedios)
    daily_stats as (
        select
            id_central,
            fecha,
            sum(generacion_real_mw) as generacion_diaria_mwh,
            max(potencia_maxima_mw) as potencia_maxima_mw,
            countif(generacion_real_mw > 0) as horas_operacion_dia,
            count(*) as horas_con_datos_dia
        from source
        group by id_central, fecha
    ),

    -- Factor de planta diario
    daily_factor as (
        select
            *,
            safe_divide(
                generacion_diaria_mwh, potencia_maxima_mw * 24
            ) as factor_planta_dia
        from daily_stats
    ),

    -- Agregación total por central
    central_metrics as (
        select
            id_central,
            count(distinct fecha) as dias_con_datos,
            sum(generacion_diaria_mwh) as generacion_total_mwh,
            round(avg(generacion_diaria_mwh), 2) as generacion_promedio_diaria_mwh,
            max(potencia_maxima_mw) as potencia_maxima_mw,
            round(avg(factor_planta_dia), 4) as factor_planta_promedio,
            round(max(factor_planta_dia), 4) as factor_planta_mejor_dia,
            sum(horas_operacion_dia) as horas_operacion_totales,
            sum(horas_con_datos_dia) as horas_con_datos_totales
        from daily_factor
        group by id_central
    ),

    -- Enriquecer con info de la central (1 fila por central)
    central_info as (
        select
            id_central,
            nombre_central,
            tipo_tecnologia,
            {{ clasificar_fuente("tipo_tecnologia") }} as clasificacion_fuente,
            region,
            comuna,
            latitude,
            longitude,
            geo_point
        from {{ ref("int_centrales_geo") }}
        where state_central = 'normal'
    ),

    final as (
        select
            ci.id_central,
            ci.nombre_central,
            ci.tipo_tecnologia,
            ci.clasificacion_fuente,
            ci.region,
            ci.comuna,
            ci.latitude,
            ci.longitude,
            ci.geo_point,
            cm.dias_con_datos,
            cm.generacion_total_mwh,
            cm.generacion_promedio_diaria_mwh,
            cm.potencia_maxima_mw,
            cm.factor_planta_promedio,
            cm.factor_planta_mejor_dia,
            cm.horas_operacion_totales,
            cm.horas_con_datos_totales,
            round(
                safe_divide(cm.horas_operacion_totales, cm.horas_con_datos_totales), 4
            ) as tasa_disponibilidad
        from central_metrics as cm
        inner join central_info as ci on cm.id_central = ci.id_central
    )

select *
from final
