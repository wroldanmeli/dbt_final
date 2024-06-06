{% macro final_get_current_date() %}

    {% set res = run_query('SELECT EXTRACT(DAY FROM CURRENT_DATE())') %}
    {% if execute %}
        {% set v = res[0][0] %}
    {% else %}
        {% set v = 0 %}
    {% endif %}
    {{ return(v) }}
 
{% endmacro %}
