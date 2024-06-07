-- depends on : {{ ref('final_tmp_process_output') }} 
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'OffFury' -%}
{%- set strprovider = 'aws' -%}
{%- set straccount_id = '' -%}

{% set full_str = namespace(str = ' ') %}
{% set full_wh1 = namespace(wh1 = ' ') %}
{% set full_wh2 = namespace(wh2 = ' ') %}
{% set get_months = namespace(meses = ' ') %}

{%- call statement('get_dates', fetch_result=True) -%}
        SELECT EXTRACT(month FROM PARSE_DATE('%F', {{ get_end_date() }})) as month_end, EXTRACT(month FROM PARSE_DATE('%F', {{ get_start_date() }}))  as month_start, EXTRACT(year FROM PARSE_DATE('%F', {{ get_end_date() }})) as year_end,  EXTRACT(year FROM PARSE_DATE('%F', {{ get_start_date() }})) as year_start  
{%- endcall -%}
{%- set states = load_result('get_dates') %}
{%- set month_end = states['data'][0][0] %}
{%- set month_start = states['data'][0][1] %}
{%- set year_end = states['data'][0][2] %}
{%- set year_start = states['data'][0][3] %}

{% set query_str %}
    select tag_key, operator, tag_value, account_id, {{ get_start_date() }} from_date, {{ get_end_date() }} to_date 
    from {{ source('ProcessedData', 'BillingdimtagsNames') }} 
    WHERE origen='{{strorigen}}' AND sustraer=true
          AND provider= '{{strprovider}}'  AND account_id != 'all' AND tag_key = 'all'
    ORDER BY tag_key
{% endset %}

{% for i in final_query_to_list(query_str) %}
       {% set straccount_id = ' OR  ( line_item_usage_account_id = ' ~ '"' ~ i.account_id ~ '"' ~ ' AND ( line_item_usage_start_date BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
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
       {% set wh1 = wh1 ~ ' OR (' ~ i.tag_key ~ ' ' ~ i.operator ~ ' "' ~ i.tag_value ~ '" AND  line_item_usage_account_id = ' ~ '"' ~ i.account_id ~ '"' ~ ' AND ( line_item_usage_start_date BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
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
       {% set wh2 = wh2 ~ ' OR (' ~ i.tag_key ~ ' ' ~ i.operator ~ ' "' ~ i.tag_value ~ '" AND  line_item_usage_account_id = ' ~ '"' ~ i.account_id ~ '"' ~ ' AND ( line_item_usage_start_date BETWEEN '  ~ '"' ~  i.from_date ~ '"' ~ ' AND ' ~ '"' ~ i.to_date ~  '"' ~ '))' %}               
       {% set full_wh2.wh2 = full_wh2.wh2  ~ wh2  %} 
{% endfor %}

{%- call statement('get_long_string', fetch_result=True) -%}
        SELECT CONCAT(' AND ( ', '{{full_str.str[4:]}}', '{{full_wh1.wh1}}', ')' ) as cadena_full  
{%- endcall -%}
{%- set states = load_result('get_long_string') %}
{%- set long_string_wh = states['data'][0][0] %}

SELECT 
        cast(timestamp_trunc(line_item_usage_start_date,DAY) as DATETIME) as datetime,
        line_item_usage_account_id as account_id
        ,product_product_name as service_type
        ,line_item_usage_type as billing_key      
        ,CASE WHEN resource_tags_user_cloudcontroller_stack= '' or resource_tags_user_cloudcontroller_stack is null THEN
          'NO-STACK'
        ELSE
          resource_tags_user_cloudcontroller_stack
        END as service_name
        ,cast(SUM(CASE WHEN line_item_line_item_type = 'DiscountedUsage' THEN 
            reservation_effective_cost
            WHEN line_item_line_item_type = 'Usage' THEN 
              line_item_unblended_cost
            WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN
              savings_plan_savings_plan_effective_cost
            WHEN line_item_line_item_type = 'Fee' 
                  AND product_product_name in ('AWS Premium Support','OCBOCBAWSDirectConnect') or bill_billing_entity = 'AWS Marketplace' THEN 
                line_item_unblended_cost
            END) as numeric) as total_cost
        ,pricing_unit as billing_unit
        ,cast(SUM(line_item_usage_amount) as numeric) as billing_usage_ammount
        ,'AWS' as provider
        ,current_date()  load_date  
        ,current_date() update_date
        ,'{{var('v_strobservation')}}' as observation
        FROM {{ source('RawData', 'billing_edp') }}  
        WHERE year >= {{year_start}}
              and year <= {{year_end}}
              and month >= {{month_start}}
              and month <=  {{month_end}}
              and date(line_item_usage_start_date) >= {{ get_start_date() }}  and date(line_item_usage_start_date) <={{ get_end_date() }} 
              AND line_item_line_item_type IN ('DiscountedUsage', 'Usage', 'RIFee', 'SavingsPlanCoveredUsage', 'Fee')
              AND line_item_usage_type NOT LIKE '%HeavyUsage%' 
              {{long_string_wh}}
        GROUP BY 1,2,3,4,5,7,9,10,11,12
        having total_cost>0
        ORDER BY 9,1

