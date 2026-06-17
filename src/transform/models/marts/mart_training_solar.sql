select
    g.id_central,
    g.timestamp_utc,
    g.fecha,
    g.lat_era5,
    g.lon_era5,
    {# g.generacion_real_mw,
    g.potencia_maxima_mw, #}
    g.factor_planta,
    e.temperatura_celsius,
    e.ghi_wm2,
    e.dni_wm2,
    e.nubosidad_pct,
    e.fraccion_directa,
    e.hora_cos,
    e.hora_sin,
    e.mes_cos,
    e.mes_sin
from {{ ref("int_generacion_solar") }} as g
inner join
    {{ ref("int_era5_solar_features") }} as e
    on g.lat_era5 = e.lat_era5
    and g.lon_era5 = e.lon_era5
    and g.timestamp_utc = e.timestamp_utc
