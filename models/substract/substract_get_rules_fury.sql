-- depends on : {{ ref('final_tmp_process_output') }}

{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'Fury' -%}
{%- set strprovider = 'Fury' -%}
{%- set intidproceso = 63 -%}
{%- set idcatalogo = 72 -%}
{%- set wh = '' -%}

{% set query %}
    select tag_key, operator, tag_value, DATE(from_date) from_date, DATE(to_date) to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='Fury' AND sustraer=true
          AND provider= 'Fury' 
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query) %}
    {% set wh = '(' ~ i.tag_key ~ ' ' ~ i.operator ~ ' ' ~ '"' ~ i.tag_value ~ '"' ~ ')' %}
    SELECT
        '{{i.tag_key}}' tag_key, 
        '{{i.tag_value}}' tag_value,
        '{{i.operator}}' operator,
        '{{i.from_date}}' from_date,
        '{{i.to_date}}' to_date,
        '{{wh}}' cadena

    {% if not loop.last %}
    union all
    {% endif %}

{% endfor %}