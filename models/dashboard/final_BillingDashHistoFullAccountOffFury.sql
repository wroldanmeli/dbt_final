{{ config(
    materialized="table",
    dataset="TemporalData",
    alias='BillingDashHistoFullAccountOffFury'
) }}

SELECT DISTINCT 
              day, 
              account_id, 
              account_name,
              total_cost, 
              conteo,
              ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_cost DESC) AS ranking
FROM {{ ref('final_tmp_BillingDashHistoFullAccountOffFury') }}  