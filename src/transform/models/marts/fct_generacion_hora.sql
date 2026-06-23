select
    id_central,
    nombre_central,
    tipo_tecnologia,
    region,
    comuna,
    provincia,
    geo_point,
    generacion_real_mw,
    potencia_maxima_mw,
    factor_planta,
    timestamp_local,
    timestamp_utc,
    fecha,
    hora,
    mes,
    anio
from {{ ref("int_generacion_enriched") }}
where state_central = 'normal'
