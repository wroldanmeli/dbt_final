-- depends on : {{ ref('substract_get_OffFury_aws'), ref('substract_get_OffFury_gcp'), ref('substract_get_OffFury_oci') }} 
{{ config(
     materialized="incremental",
        incremental_strategy = "merge",
        alias='BillingtabOffFurysubtract',
        dataset='ProcessedData',
        unique_key=[
              "datetime",
              "account_id", 
              "service_type",
              "billing_key",
              "service_name",   
              "billing_unit", 
              "provider"
        ]
)}}

SELECT * FROM {{ ref('substract_get_OffFury_aws') }} 
UNION ALL 
SELECT * FROM {{ ref('substract_get_OffFury_gcp') }}
UNION ALL 
SELECT * FROM {{ ref('substract_get_OffFury_oci') }}

