{{ config(
    materialized="table",
    tag='TemporalData',
    alias='BillingDashHistoRankingOffFury'
) }}

SELECT account_name, ARRAY_AGG(STRUCT (day, ranking, conteo, total_cost )) histo_rank 
          FROM {{ ref('final_BillingDashControlBaseOffFury') }}
    GROUP BY account_name