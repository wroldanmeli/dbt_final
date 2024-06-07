-- depends_on: {{ ref('final_last_snapshot') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set prev_snapshot_suffix_name = load_result('get_sufijo')['data'][0][0] -%}

            SELECT 
                    '{{prev_snapshot_suffix_name}}' sufijo, 
                    CURRENT_DATETIME('UTC') datetime,
                    COUNT(1) records,
                    COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
            FROM {{ ref('final_BillingDashControlBase') }}  
            WHERE day BETWEEN '{{ var("v_fecha_start") }}' AND  '{{ var("v_fecha_end") }}'
