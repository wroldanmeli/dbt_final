
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_prev_dashfury_values`
      
    
    

    OPTIONS()
    as (
      -- depends_on: `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`
SELECT '20240607153300' sufijo, 
                PARSE_DATETIME('%Y%m%d%H%M%S', '20240607153300') datetime,
                COUNT(1) records,
                COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost 
          FROM ProcessedData_REPRO_snapshots.BillingDashControlBase_20240607153300
          WHERE day BETWEEN '2024-06-01' AND '2024-06-04'
    );
  