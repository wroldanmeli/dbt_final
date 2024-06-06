{# Just a macro to change the default behavior on the schema names defined on DBT
   https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro get_start_date() -%}
    {%- if var("v_fecha_start") == "None" -%}

        DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

    {%- else -%}

        '{{ var("v_fecha_start") }}'

    {%- endif -%}

{%- endmacro %}