
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
                day BETWEEN  '2024-06-01' 
                AND '2024-06-04'
                EXCEPT 
                DISTINCT 
                SELECT day,
                    idproceso,
                    proceso,
                    conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
                FROM 
                ProcessedData_REPRO_snapshots.BillingDashControlBase_20240607013700 
                WHERE 
                day BETWEEN '2024-06-01' 
                AND '2024-06-04'
            ),
            
             dif_2 AS (
                 SELECT day,
                 idproceso,
                 proceso,
                 conteo,
                 COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost 
        FROM 
          ProcessedData_REPRO_snapshots.BillingDashControlBase_20240607013700 
        WHERE 
          day BETWEEN '2024-06-01' 
          AND '2024-06-04' 
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
          day BETWEEN '2024-06-01' 
          AND '2024-06-04'
      )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2