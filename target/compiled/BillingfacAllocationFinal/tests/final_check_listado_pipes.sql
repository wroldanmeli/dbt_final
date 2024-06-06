

WITH 
    listado_prod AS (SELECT DISTINCT idprocesoanterior FROM ProcessedData.BillingfacAllocationFinal 
                                       WHERE DATE(datetime) BETWEEN "2024-01-01" AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY)),
    
    listado_pipe  AS (SELECT DISTINCT idprocesoanterior FROM ProcessedData.BillingfacAllocationFinal  B 
                                       WHERE DATE(datetime) BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY)  AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) ) ,  
    
    list_pipe_validos AS (SELECT A.idproceso FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` A 
     WHERE EXISTS (SELECT 'X' FROM  listado_prod B 
            WHERE A.idproceso=B.idprocesoanterior) )
     
     SELECT A.idproceso FROM list_pipe_validos  A  WHERE NOT EXISTS( (SELECT 'X' FROM   listado_pipe B
            WHERE A.idproceso=B.idprocesoanterior) )