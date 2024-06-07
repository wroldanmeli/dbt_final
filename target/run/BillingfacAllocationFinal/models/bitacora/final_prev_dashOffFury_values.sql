
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_prev_dashOffFury_values`
      
    
    

    OPTIONS()
    as (
      -- depends_on: `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`
SELECT '20240607013700' sufijo,
                PARSE_DATETIME('%Y%m%d%H%M%S', '20240607013700') datetime,
                COUNT(1) records,
                COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost
          FROM ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_20240607013700
          WHERE day BETWEEN '2024-06-01' AND '2024-06-04'
    );
  