
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_change_fury`
      
    
    

    OPTIONS()
    as (
      
-- depends on : (<BigQueryRelation `metrics-streams-dev`.`billing_wr_test`.`BillingDashHistoRankingOffFury`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`>)  
WITH dif_1 AS (
                SELECT day,
                    idproceso,
                    proceso,
                    conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost  
                FROM 
                `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBase`
                WHERE 
                day BETWEEN  DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) 
                AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) 
                EXCEPT 
                DISTINCT 
                SELECT day,
                    idproceso,
                    proceso,
                    conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
                FROM 
                ProcessedData_REPRO_snapshots.BillingDashControlBase_20240606173900 
                WHERE 
                day BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) 
                AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
            ),
            
             dif_2 AS (
                 SELECT day,
                 idproceso,
                 proceso,
                 conteo,
                 COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
        FROM 
          ProcessedData_REPRO_snapshots.BillingDashControlBase_20240606173900 
        WHERE 
          day BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) 
          AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) 
        EXCEPT 
          DISTINCT 
        SELECT day,
               idproceso,
               proceso,
               conteo,
               COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost         
        FROM
          `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBase`
        WHERE 
          day BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) 
          AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
      )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2
    );
  