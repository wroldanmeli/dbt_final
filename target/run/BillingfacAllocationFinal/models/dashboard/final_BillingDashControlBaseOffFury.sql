
  
    

    create or replace table `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBaseOffFury`
      
    
    

    OPTIONS()
    as (
      

   WITH     
    selected_base AS (
      SELECT DATE(day) day,
        account_id, 
        account_name,
        total_cost,
        conteo
      FROM `metrics-streams-dev`.`TemporalData`.`BillingDashHistoFullAccountOffFury`  
      QUALIFY ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_cost DESC) <= 20
      UNION ALL
      SELECT day, 
             'ID_OTHERS' account_id, 
             'OTHERS ACCOUNTS TOP 20' account_name, 
             ROUND(SUM(total_cost),2) total_cost,
             SUM(conteo) conteo
      FROM (
            SELECT DATE(day) day,
                    account_id,
                    account_name,
                    total_cost,
                    conteo
            FROM  `metrics-streams-dev`.`TemporalData`.`BillingDashHistoFullAccountOffFury`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_cost DESC) > 20)
      GROUP BY 1,2,3  ) 

    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY total_cost DESC) AS ranking 
    FROM selected_base
    ORDER BY 1 DESC, 4 DESC
    );
  