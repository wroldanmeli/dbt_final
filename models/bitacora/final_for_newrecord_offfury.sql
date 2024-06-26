-- depends_on: {{ ref('final_last_snapshot'), ref('final_change_offfury') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}
        
        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set sufijo = load_result('get_sufijo')['data'][0][0] -%}

             SELECT   
                '{{sufijo}}'  sufijo, 
                ARRAY_AGG(
                STRUCT (
                    COALESCE(day,null,DATE(CURRENT_DATE())) AS day,
                    COALESCE(account_id,null,'EMPTY') AS account_id,
                    COALESCE(account_name,null,'EMPTY') AS account_name,
                    'N/A' AS responsable,
                    COALESCE(sentido,null,'EMPTY') AS sentido,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost,
                    COALESCE(CAST(conteo AS INT64),null,0) AS conteo,
                    COALESCE(CAST(ranking AS INT64),null,0) AS ranking
                    )) AS value   
            FROM (SELECT CURRENT_DATE() day, 
                  'NULL' account_id,
                  'NULL' account_name, 
                  'N/A' responsable,
                  'NULL' sentido,
                  0 total_cost,
                  0 conteo, 
                  0 ranking)       