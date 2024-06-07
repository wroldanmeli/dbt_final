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
                day BETWEEN  {{ get_start_date() }} 
                AND {{ get_end_date() }}
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
                day BETWEEN {{ get_start_date() }} 
                AND {{ get_end_date() }}
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
          day BETWEEN {{ get_start_date() }} 
          AND {{ get_end_date() }} 
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
          day BETWEEN {{ get_start_date() }} 
          AND {{ get_end_date() }}
      )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2

 