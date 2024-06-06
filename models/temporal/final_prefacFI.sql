-- depends_on: {{ ref('final_get_finance_limits') }}
{{ config(
    materialized="table"
)}}

WITH finance_limits AS (
    SELECT output.start_date start_date, 
           output.end_date end_date 
    FROM {{ ref('final_get_finance_limits') }}
)

SELECT A.* FROM {{ ref('final_prefacIT_nodup') }} A , finance_limits B
WHERE  DATE(A.datetime) BETWEEN B.start_date AND B.end_date 