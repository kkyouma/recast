{% macro clasificar_fuente(tipo_tecnologia_col) %}
    case
        when
            {{ tipo_tecnologia_col }} in (
                'Solar',
                'Eolica',
                'Mini Hidráulica de Pasada',
                'Hidráulica de Pasada',
                'Geotérmica',
                'Biomasa',
                'Biogás'
            )
        then 'Renovable'
        else 'Convencional'
    end
{% endmacro %}
