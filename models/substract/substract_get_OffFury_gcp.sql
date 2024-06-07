-- depends on : {{ ref('final_tmp_process_output') }} 
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'OffFury' -%}
{%- set strprovider = 'gcp' -%}
{%- set straccount_id = '' -%}

{% set full_str = namespace(str = ' ') %}
{% set full_wh1 = namespace(wh1 = ' ') %}
{% set full_wh2 = namespace(wh2 = ' ') %}
{% set get_months = namespace(meses = ' ') %}


{% set query_str %}
    select tag_key, operator, tag_value, account_id, {{ get_start_date() }} from_date, {{ get_end_date() }} to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='{{strorigen}}' AND sustraer=true
          AND provider= '{{strprovider}}'  AND account_id != 'all' AND tag_key = 'all'
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query_str) %}
       {% set straccount_id = ' OR  (  PROJECT.id = ' ~ '"' ~ i.account_id ~ '"' ~ ' AND ( usage_start_time BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
       {% set full_str.str = full_str.str  ~ straccount_id  %} 
{% endfor %}

{% set query_wh1 %}
    select tag_key, operator, tag_value, account_id, {{ get_start_date() }} from_date, {{ get_end_date() }} to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='{{strorigen}}' AND sustraer=true
          AND provider= '{{strprovider}}'  AND account_id != 'all' AND tag_key != 'all'
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query_wh1) %}
       {% set wh1 = wh1 ~ ' OR (' ~ i.tag_key ~ ' ' ~ i.operator ~ ' "' ~ i.tag_value ~ '" AND PROJECT.id = ' ~ '"' ~ i.account_id ~ '"' ~ ' AND ( usage_start_time BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
       {% set full_wh1.wh1 = full_wh1.wh1 ~ wh1  %} 
{% endfor %}

{% set query_wh2 %}
    select tag_key, operator, tag_value, account_id, {{ get_start_date() }} from_date, {{ get_end_date() }} to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='{{strorigen}}' AND sustraer=true
          AND provider= '{{strprovider}}'  AND account_id = 'all' 
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query_wh2) %}
       {% set wh2 = wh2 ~ ' OR (' ~ i.tag_key ~ ' ' ~ i.operator ~ ' "' ~ i.tag_value ~ '"'  ~ ' AND ( usage_start_time BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
       {% set full_wh2.wh2 = full_wh2.wh2  ~ wh2  %} 
{% endfor %}

{%- call statement('get_long_string', fetch_result=True) -%}
        SELECT CONCAT(' AND ( ', '{{full_str.str[4:]}}', '{{full_wh1.wh1}}', ')' ) as cadena_full  
{%- endcall -%}
{%- set states = load_result('get_long_string') %}
{%- set long_string_wh = states['data'][0][0] %}

SELECT
      cast(TIMESTAMP_TRUNC(usage_start_time, DAY) AS datetime) datetime,
      CASE
        WHEN PROJECT.id IS NULL THEN 'Cargos no especificos de un proyecto'
        ELSE
          PROJECT.id
      END AS account_id,
      service.description AS service_type,
      sku.description AS billing_key,
      'NO-STACK' AS service_name,
      cast((SUM(CAST(cost * 1000000 AS INT64)) + SUM(IFNULL((
              SELECT
                SUM(CAST(c.amount * 1000000 AS INT64))
              FROM
                UNNEST(credits) c),
          0))) / 1000000 as numeric)  AS total_cost,
      usage.pricing_unit AS billing_unit,
      cast(SUM(usage.amount_in_pricing_units) as numeric)  AS billing_usage_amount,
      'GCP' AS provider
      ,current_date()  load_date	
      ,current_date() update_date
      ,'{{var('v_strobservation')}}' as observation 
    FROM {{ source('GoogleCloudBillingDetail', 'BillingDetailExport') }} 
    WHERE
      PARTITIONTIME >= DATETIME_SUB(CAST(PARSE_DATE('%F', {{ get_start_date() }}) as TIMESTAMP), INTERVAL 1 DAY)
      AND PARTITIONTIME <= DATETIME_ADD(cast(PARSE_DATE('%F', {{ get_end_date() }}) as TIMESTAMP ),INTERVAL 1 DAY)
      AND TIMESTAMP_TRUNC(usage_start_time, DAY) >=cast(PARSE_DATE('%F', {{ get_start_date() }}) as timestamp)
      AND TIMESTAMP_TRUNC(usage_start_time, DAY) <= cast(PARSE_DATE('%F', {{ get_end_date() }}) as timestamp)
      {{long_string_wh}}
    GROUP BY 1, 2, 3, 4, 5, 7, 9, 10, 11, 12
    HAVING total_cost > 0.0


