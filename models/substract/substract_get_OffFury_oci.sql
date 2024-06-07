-- depends on : {{ ref('final_tmp_process_output') }} 
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}


{%- set strorigen = 'OffFury' -%}
{%- set strprovider = 'oci' -%}
{%- set straccount_id = '' -%}

{% set full_str = namespace(str = ' ') %}
{% set full_wh1 = namespace(wh1 = ' ') %}
{% set full_wh2 = namespace(wh2 = ' ') %}
{% set get_months = namespace(meses = ' ') %}

{% set query_wh1 %}
    select tag_key, operator, tag_value, account_id, '{{var('v_fecha_start')}}' from_date, '{{var('v_fecha_end')}}' to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='{{strorigen}}' AND sustraer=true
          AND provider= '{{strprovider}}'  AND account_id != 'all' AND tag_key != 'all'
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query_wh1) %}
       {% set wh1 = wh1 ~ ' OR (' ~ i.tag_key ~ ' ' ~ i.operator ~ ' "' ~ i.tag_value ~ '" ' ~ ' AND  cost_lineItem_intervalUsageStart BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ ')' %}
          {% set full_wh1.wh1 = full_wh1.wh1 ~ wh1  %} 
{% endfor %}

{%- call statement('get_long_string', fetch_result=True) -%}
        SELECT '{{full_wh1.wh1[4:]}}' as cadena_full  
{%- endcall -%}
{%- set states = load_result('get_long_string') %}
{%- set long_string_wh = states['data'][0][0] %}

SELECT
      cast(TIMESTAMP_TRUNC(cost_lineItem_intervalUsageStart, DAY) AS datetime) datetime,
     'ocid1.tenancy.oc1..aaaaaaaalgfveinmsacv4lo36wk4hfoiokh2m52j72sw5f4boms676vvc2da' AS account_id,
      CONCAT('OCI_',cost_product_Description) AS service_type,
      COALESCE(usage_product_resource,null,'UNDEFINED')  AS billing_key,
      'NO-STACK' AS service_name,
      CAST(SUM(ROUND(cost_total_cost_US,2)) AS NUMERIC)  AS total_cost,
      COALESCE(usage_consumedQuantityUnits,null,'UNDEFINED') billing_unit,
      CAST(SUM(ROUND(cost_total_usage,2)) AS NUMERIC)  AS billing_usage_amount,
      'OCI' AS provider
      ,current_date()  load_date
      ,current_date() update_date
      ,'{{var('v_strobservation')}}' as observation
    FROM {{ source('WorkData', 'BillingOracleAnnualUnified') }}
    WHERE
       {{long_string_wh}}
    GROUP BY 1,2,3,4,5,7,9,10,11,12
    HAVING total_cost > 0.0


