{# Obtiene las Diferencias entre las tablas last y prev para el grupo Fury  para la Bitacora de reprocesos#}
-- depends on : {{ ref('final_BillingDashHistoRankingOffFury'), ref('final_last_snapshot')  }}  
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set prev_snapshot_suffix_name = load_result('get_sufijo')['data'][0][0] -%}
        {%- set prev_snapshot_dashfury_name = 'ProcessedData_REPRO_snapshots.BillingDashControlBase_' ~  prev_snapshot_suffix_name -%}

        WITH dif_1 AS (
                SELECT day,
                    idproceso,
                    proceso,
                    conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost  
                FROM 
                {{ref('final_BillingDashControlBase')}}
                WHERE 
                day BETWEEN  '{{ var("v_fecha_start") }}' 
                AND '{{ var("v_fecha_end") }}'
                EXCEPT 
                DISTINCT 
                SELECT day,
                    idproceso,
                    proceso,
                    conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
                FROM 
                {{prev_snapshot_dashfury_name}} 
                WHERE 
                day BETWEEN '{{ var("v_fecha_start") }}' 
                AND '{{ var("v_fecha_end") }}'
            ),
            
             dif_2 AS (
                 SELECT day,
                 idproceso,
                 proceso,
                 conteo,
                 COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
        FROM 
          {{prev_snapshot_dashfury_name}} 
        WHERE 
          day BETWEEN '{{ var("v_fecha_start") }}' 
          AND '{{ var("v_fecha_end") }}' 
        EXCEPT 
          DISTINCT 
        SELECT day,
               idproceso,
               proceso,
               conteo,
               COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost         
        FROM
          {{ref('final_BillingDashControlBase')}}
        WHERE 
          day BETWEEN '{{ var("v_fecha_start") }}' 
          AND '{{ var("v_fecha_end") }}'
      )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2

 