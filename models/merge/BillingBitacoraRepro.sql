-- depends on : {{ ref('final_new_record_bitacora') }}  
{{ config(
    materialized="incremental",
        incremental_strategy = "merge",
        unique_key=[
              "sufijo"
        ]
)}}

        {%- call statement('get_columns', fetch_result=True) -%}
            SELECT STRING_AGG(column_name) FROM ProcessedData.INFORMATION_SCHEMA.COLUMNS WHERE table_name =  'BillingBitacoraRepro' ;
        {%- endcall -%}
        {%- set fields_bita = load_result('get_columns')['data'][0][0] -%}
        {{ print('fields_bita: ' ~  fields_bita) }}


SELECT * FROM {{ ref('final_new_record_bitacora') }} 
