

SELECT account_name, ARRAY_AGG(STRUCT (day, ranking, conteo, total_cost )) histo_rank 
          FROM `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBaseOffFury`
    GROUP BY account_name