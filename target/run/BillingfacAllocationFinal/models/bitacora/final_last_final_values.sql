
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_last_final_values`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`  
SELECT 
                            '20240606173900'  sufijo, 
                            CURRENT_DATETIME('UTC') datetime,
                            COUNT(1) records,
                            COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
                          FROM ProcessedData.BillingfacAllocationFinal 
                          WHERE datetime BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) AND  DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
    );
  