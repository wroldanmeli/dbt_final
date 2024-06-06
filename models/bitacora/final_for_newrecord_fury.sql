-- depends_on: {{ ref('final_last_snapshot'), ref('final_change_fury') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}
        
        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set sufijo = load_result('get_sufijo')['data'][0][0] -%}

SELECT   
                '{{sufijo}}' sufijo, 
                ARRAY_AGG(
                STRUCT (
                    COALESCE(day,null,DATE(CURRENT_DATE())) AS day,
                    COALESCE(idproceso,null,0) AS idproceso,
                    COALESCE(proceso,null,'EMPTY') AS proceso,
                    COALESCE(sentido,null,'EMPTY') AS sentido,
                    COALESCE(CAST(conteo AS FLOAT64),null,0) AS conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost
                    )) AS value 

               FROM (SELECT CURRENT_DATE() day, 
                    0 idproceso,
                    'NULL' proceso, 
                    'NULL' sentido,
                    0 conteo, 
                    0 total_cost)
        