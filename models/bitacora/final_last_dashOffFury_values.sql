-- depends_on: {{ ref('final_last_snapshot') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }};
        {%- endcall -%}
        {%- set prev_snapshot_suffix_name = load_result('get_sufijo')['data'][0][0] -%}
        {%- set prev_snapshot_dashOffFury_name = 'ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_' ~  prev_snapshot_suffix_name -%}
    
            SELECT 
                    '{{prev_snapshot_suffix_name}}' sufijo, 
                    CURRENT_DATETIME('UTC') datetime,
                    COUNT(1) records,
                    COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
            FROM  ProcessedData.BillingDashControlBaseOffFury
            WHERE day BETWEEN {{ get_start_date() }} AND  {{ get_end_date() }}