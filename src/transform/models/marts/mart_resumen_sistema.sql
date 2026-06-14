{{ config(partition_by={"field": "fecha", "data_type": "date"}) }}

with
    source as (
        select *
        from {{ ref("int_generacion_enriched") }}
        where state_central = 'normal'
    ),

    -- Capacidad instalada total del sistema por día:
    -- MAX por central, luego SUM de todas las centrales
    capacidad_sistema as (
        select fecha, sum(max_potencia) as capacidad_instalada_total_mw
        from
            (
                select fecha, id_central, max(potencia_maxima_mw) as max_potencia
                from source
                group by fecha, id_central
            )
        group by fecha
    ),

    -- Generación horaria del sistema completo (para pico/valle)
    sistema_horario as (
        select fecha, hora, sum(generacion_real_mw) as generacion_sistema_mw
        from source
        group by fecha, hora
    ),

    pico_valle as (
        select
            fecha,
            max(generacion_sistema_mw) as demanda_maxima_mw,
            min(generacion_sistema_mw) as demanda_minima_mw
        from sistema_horario
        group by fecha
    ),

    -- Tecnología con mayor aporte por día
    generacion_por_tech as (
        select fecha, tipo_tecnologia, sum(generacion_real_mw) as gen_tech_mwh
        from source
        group by fecha, tipo_tecnologia
    ),

    top_tech as (
        select fecha, tipo_tecnologia as top_tecnologia
        from
            (
                select
                    fecha,
                    tipo_tecnologia,
                    gen_tech_mwh,
                    row_number() over (
                        partition by fecha order by gen_tech_mwh desc
                    ) as rn
                from generacion_por_tech
            )
        where rn = 1
    ),

    -- Métricas principales del día
    daily_system as (
        select
            fecha,

            -- Generación total
            sum(generacion_real_mw) as generacion_total_mwh,

            -- Renovable vs Convencional
            sum(
                case
                    when clasificacion_fuente = 'Renovable'
                    then generacion_real_mw
                    else 0
                end
            ) as generacion_renovable_mwh,
            sum(
                case
                    when clasificacion_fuente = 'Convencional'
                    then generacion_real_mw
                    else 0
                end
            ) as generacion_convencional_mwh,

            -- Centrales activas
            count(
                distinct case when generacion_real_mw > 0 then id_central end
            ) as num_centrales_activas,
            count(
                distinct case
                    when generacion_real_mw > 0 and clasificacion_fuente = 'Renovable'
                    then id_central
                end
            ) as num_centrales_renovables_activas
        from source
        group by fecha
    ),

    final as (
        select
            ds.fecha,
            ds.generacion_total_mwh,
            ds.generacion_renovable_mwh,
            ds.generacion_convencional_mwh,
            round(
                safe_divide(ds.generacion_renovable_mwh, ds.generacion_total_mwh) * 100,
                2
            ) as pct_renovable,
            round(
                safe_divide(ds.generacion_convencional_mwh, ds.generacion_total_mwh)
                * 100,
                2
            ) as pct_convencional,
            pv.demanda_maxima_mw,
            pv.demanda_minima_mw,
            ds.num_centrales_activas,
            ds.num_centrales_renovables_activas,
            cs.capacidad_instalada_total_mw,
            round(
                safe_divide(
                    ds.generacion_total_mwh, cs.capacidad_instalada_total_mw * 24
                ),
                4
            ) as factor_planta_sistema,
            tt.top_tecnologia
        from daily_system as ds
        inner join pico_valle as pv on ds.fecha = pv.fecha
        inner join capacidad_sistema as cs on ds.fecha = cs.fecha
        inner join top_tech as tt on ds.fecha = tt.fecha
    )

select *
from final
