-- depends_on: {{ ref('final_prefacIT_nodup')}} 
{{
    config(
        materialized="incremental",
        dataset='ProcessedData',
        incremental_strategy = "merge",
        unique_key=[
              "datetime",
              "generator_cost_name", 
              "account_id", 
              "service_type",
              "billing_key",
              "service_name",   
              "billing_unit", 
              "billing_concept",
              "idprocesoanterior"
        ],
    pre_hook=before_begin("DELETE FROM ProcessedData.BillingfacAllocationFinal WHERE idprocesoanterior IN (SELECT idproceso FROM  {{ ref('final_tmp_process_output')}})  AND DATE(datetime) BETWEEN {{var('v_fecha_start') }} AND {{var('v_fecha_end')}} ")
    )
}}

SELECT * FROM {{ ref('final_prefacIT_nodup')}}