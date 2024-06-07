
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_fury`
      
    
    

    OPTIONS()
    as (
      -- depends_on: (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_change_fury`>)
SELECT   
                '20240607153300' sufijo, 
                ARRAY_AGG(
                STRUCT (
                    COALESCE(day,null,DATE(CURRENT_DATE())) AS day,
                    COALESCE(idproceso,null,0) AS idproceso,
                    COALESCE(proceso,null,'EMPTY') AS proceso,
                    COALESCE(sentido,null,'EMPTY') AS sentido,
                    COALESCE(CAST(conteo AS FLOAT64),null,0) AS conteo,
                    COALESCE(CAST(total_cost AS FLOAT64),null,0) AS total_cost
                    )) AS value 

               FROM (SELECT CURRENT_DATE() day, 
                    0 idproceso,
                    'NULL' proceso, 
                    'NULL' sentido,
                    0 conteo, 
                    0 total_cost)
    );
  