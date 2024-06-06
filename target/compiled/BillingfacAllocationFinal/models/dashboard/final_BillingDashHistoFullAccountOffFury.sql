

SELECT DISTINCT 
              day, 
              account_id, 
              account_name,
              total_cost, 
              conteo,
              ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_cost DESC) AS ranking
FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingDashHistoFullAccountOffFury`