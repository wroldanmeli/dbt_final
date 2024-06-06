{{
    config(
        alias="BillingfacAllocationFinal_Finance",
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
        ]    )
}}

SELECT * FROM {{ ref('final_prefacFI')}}
