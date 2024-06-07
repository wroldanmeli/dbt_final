-- depends on : (<BigQueryRelation `metrics-streams-dev`.`billing_wr_test`.`BillingDashHistoRankingOffFury`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`>)  


        WITH dif_1 AS (
                SELECT * 
                FROM `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBaseOffFury`
                WHERE 
                day BETWEEN  '2024-06-01' 
                AND '2024-06-04' 
                EXCEPT 
                DISTINCT 
                SELECT * 
                FROM 
                ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_20240607153300 
                WHERE 
                day BETWEEN '2024-06-01' 
                AND '2024-06-04'
            ),
            
             dif_2 AS (
                SELECT * 
                FROM 
                ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_20240607153300 
                WHERE 
                day BETWEEN '2024-06-01' 
                AND '2024-06-04' 
                EXCEPT 
                DISTINCT 
                SELECT *         
                FROM
                `metrics-streams-dev`.`ProcessedData`.`BillingDashControlBaseOffFury`
                WHERE 
                day BETWEEN '2024-06-01' 
                AND '2024-06-04'
            )
        
        SELECT 'LasttoPrev' sentido, * FROM dif_1
        UNION ALL
        SELECT 'PrevtoLast' sentido, * FROM dif_2