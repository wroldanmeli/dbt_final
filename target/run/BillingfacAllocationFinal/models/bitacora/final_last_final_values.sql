
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_last_final_values`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`  
SELECT 
                            '20240607153300'  sufijo, 
                            CURRENT_DATETIME('UTC') datetime,
                            COUNT(1) records,
                            COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
                          FROM ProcessedData.BillingfacAllocationFinal 
                          WHERE datetime BETWEEN '2024-06-01' AND  '2024-06-04'
    );
  