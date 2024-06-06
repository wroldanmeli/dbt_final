{# Just a macro to change the default behavior on the schema names defined on DBT
   https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro get_end_date() -%}
    {%- if var("v_fecha_end") == "None" -%}

        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

    {%- else -%}

        '{{ var("v_fecha_end") }}'

    {%- endif -%}

{%- endmacro %}