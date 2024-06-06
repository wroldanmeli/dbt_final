
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`
      
    
    

    OPTIONS()
    as (
      -- depends_on: `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output`
SELECT DATE(datetime) day_snap, '20240606173900' sufijo , COUNT(1) records_snap, CAST(ROUND(SUM(total_cost),0) AS INT64) cost_snap 
        FROM ProcessedData_REPRO_snapshots.BillingfacAllocationFinal_20240606173900  
        GROUP BY 1
    );
  