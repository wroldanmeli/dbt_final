{# Recopila los registros del proceso desde todas las tablas intermedias antes de la eliminacion 
   de duplicados y posterior insercion en la tabla final #}
-- depends on : {{ ref('final_tmp_BillingfacgetOffFury'), ref('final_tmp_BillingfacgetFury'), ref('final_tmp_BillingfacgetMelidata'), ref('final_tmp_BillingfacgetBigquery'), ref('final_tmp_BillingfacgetOpenAI'), ref('final_tmp_BillingfacgetDatamesh'), final_tmp_BillingtabMelidataBIFinalCosts }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

SELECT * FROM {{ ref('final_tmp_BillingfacgetOffFury') }}
UNION ALL 
SELECT * FROM {{ ref('final_tmp_BillingfacgetFury') }}
UNION ALL 
SELECT * FROM {{ ref('final_tmp_BillingfacgetMelidata') }}
UNION ALL 
SELECT * FROM {{ ref('final_tmp_BillingfacgetBigquery') }}
UNION ALL
SELECT * FROM {{ ref('final_tmp_BillingfacgetOpenAI') }}
UNION ALL
SELECT * FROM {{ ref('final_tmp_BillingfacgetDatamesh') }}
UNION ALL
SELECT * FROM {{ ref('final_tmp_BillingtabMelidataBIFinalCosts') }}