-- depends on : {{ ref('final_last_snapshot') }}  
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
                            CURRENT_DATETIME('UTC') datetime,
                            COUNT(1) records,
                            COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
                          FROM ProcessedData.BillingfacAllocationFinal 
                          WHERE datetime BETWEEN {{var('v_fecha_start')}} AND  {{var('v_fecha_end')}}