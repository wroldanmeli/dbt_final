
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_offfury`
      
    
    

    OPTIONS()
    as (
      -- depends_on: (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_change_offfury`>)
SELECT   
                '20240607013700'  sufijo, 
                ARRAY_AGG(
                STRUCT (
                    COALESCE(day,null,DATE(CURRENT_DATE())) AS day,
                    COALESCE(account_id,null,'EMPTY') AS account_id,
                    COALESCE(account_name,null,'EMPTY') AS account_name,
                    'N/A' AS responsable,
                    COALESCE(sentido,null,'EMPTY') AS sentido,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost,
                    COALESCE(CAST(conteo AS INT64),null,0) AS conteo,
                    COALESCE(CAST(ranking AS INT64),null,0) AS ranking
                    )) AS value   
            FROM (SELECT CURRENT_DATE() day, 
                  'NULL' account_id,
                  'NULL' account_name, 
                  'N/A' responsable,
                  'NULL' sentido,
                  0 total_cost,
                  0 conteo, 
                  0 ranking)
    );
  