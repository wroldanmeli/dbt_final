-- depends on : {{ ref('final_BillingDashHistoRankingOffFury'), ref('final_last_snapshot') }}  
{{ config(
    materialized="table",
    dataset='TemporalData'

)}}

        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set prev_snapshot_suffix_name = load_result('get_sufijo')['data'][0][0] -%}
        {%- set prev_snapshot_dashOffFury_name = 'ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_' ~  prev_snapshot_suffix_name -%}
        {{ print('prev_snapshot_dashOffFury_name: ' ~  prev_snapshot_dashOffFury_name) }}

        WITH dif_1 AS (
                SELECT * 
                FROM {{ref('final_BillingDashControlBaseOffFury')}}
                WHERE 
                day BETWEEN  {{ get_start_date() }} 
                AND {{ get_end_date() }} 
                EXCEPT 
                DISTINCT 
                SELECT * 
                FROM 
                {{prev_snapshot_dashOffFury_name}} 
                WHERE 
                day BETWEEN {{ get_start_date() }} 
                AND {{ get_end_date() }}
            ),
            
             dif_2 AS (
                SELECT * 
                FROM 
                {{prev_snapshot_dashOffFury_name}} 
                WHERE 
                day BETWEEN {{ get_start_date() }} 
                AND {{ get_end_date() }} 
                EXCEPT 
                DISTINCT 
                SELECT *         
                FROM
                {{ref('final_BillingDashControlBaseOffFury')}}
                WHERE 
                day BETWEEN {{ get_start_date() }} 
                AND {{ get_end_date() }}
            )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2

 