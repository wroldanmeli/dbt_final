-- depends_on: {{ ref('final_last_snapshot') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

        {%- call statement('get_sufijo', fetch_result=True) -%}
            SELECT DISTINCT sufijo FROM {{ ref('final_last_snapshot') }} ;
        {%- endcall -%}
        {%- set prev_snapshot_suffix_name = load_result('get_sufijo')['data'][0][0] -%}
        {%- set datetime_format = '%Y%m%d%H%M%S' -%}
        {%- set prev_snapshot_table_name = 'ProcessedData_REPRO_snapshots.BillingfacAllocationFinal_' ~  prev_snapshot_suffix_name -%}
  
          SELECT '{{prev_snapshot_suffix_name}}' sufijo, 
                PARSE_DATETIME('{{datetime_format}}', '{{prev_snapshot_suffix_name}}') datetime,
                COUNT(1) records,
                COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost 
          FROM {{prev_snapshot_table_name}}
          WHERE datetime BETWEEN '{{ var("v_fecha_start") }}' AND '{{ var("v_fecha_end") }}'