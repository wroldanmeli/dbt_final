-- depends_on: {{ ref('final_prefacIT') }}
{{ config(
    materialized="ephemeral"
)}}

{% set CURRENT_DATE %}
   {# invoke macro in order to get current_date as int #}
   SELECT CAST({{ final_get_current_date() }} AS INT64)  AS day
{% endset %}
{% set results = run_query(CURRENT_DATE) %}
{% if execute %}
    {% set current_date = results.columns[0][0] %}
    {{print("current_date " ~ current_date)}}
{% endif %}

WITH get_current_day AS (
    SELECT CASE WHEN CAST('{{ current_date }}' AS INT64) >= 20 THEN 
         DATE(DATE_TRUNC(CURRENT_DATE(), month) )
    ELSE
         DATE(DATE_TRUNC(CURRENT_DATE(), month) - INTERVAL 1 MONTH )
    END AS limit_finance),

    get_range AS (SELECT limit_finance, 
           '{{ var("v_fecha_start") }}' start_date, 
           '{{ var("v_fecha_end") }}' end_date 
    FROM get_current_day), 

    get_limits_ind AS (
    SELECT 
        CASE 
            WHEN  PARSE_DATE('%F', A.start_date) >= B.limit_finance AND PARSE_DATE('%F', A.end_date) >= B.limit_finance THEN
                STRUCT(PARSE_DATE('%F', A.start_date) as start_date, PARSE_DATE('%F', A.end_date) as end_date) 
            WHEN  PARSE_DATE('%F', A.start_date) < B.limit_finance AND PARSE_DATE('%F', A.end_date) < B.limit_finance THEN
                STRUCT(DATE(CURRENT_DATE('UTC') + INTERVAL 100 DAY) as start_date, DATE(CURRENT_DATE('UTC') + INTERVAL 100 DAY) as end_date) 
            WHEN  PARSE_DATE('%F', A.start_date) < B.limit_finance AND PARSE_DATE('%F', A.end_date) >= B.limit_finance THEN
                STRUCT(B.limit_finance as start_date, DATE(A.end_date) as end_date ) 
       END AS output  
    FROM get_range A , get_current_day B )

    SELECT * FROM get_limits_ind
    WHERE output IS NOT NULL
