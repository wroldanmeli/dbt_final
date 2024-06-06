
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_last_dashOffFury_values`
      
    
    

    OPTIONS()
    as (
      -- depends_on: `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`
SELECT 
                    '20240606173900' sufijo, 
                    CURRENT_DATETIME('UTC') datetime,
                    COUNT(1) records,
                    COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
            FROM  ProcessedData.BillingDashControlBaseOffFury
            WHERE day BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) AND  DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
    );
  