WITH 



  lista_proc AS (SELECT idproceso FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output`),

  days_dummies AS (
        SELECT days_gen FROM 
        UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE('%F', '2024-06-01'), PARSE_DATE('%F', '2024-06-01')))  AS days_gen
        ),
  
  matriz_days_pipes AS (
       SELECT * 
       FROM days_dummies, lista_proc 
       ),


  lista_validos AS (SELECT idprocesoanterior FROM `metrics-streams-dev`.`ProcessedData`.`BillingfacAllocationFinal` 
                    WHERE DATE(datetime) BETWEEN PARSE_DATE('%F', '2024-06-01') - INTERVAL 7 DAY  
                          AND PARSE_DATE('%F', '2024-06-01') - INTERVAL 1 DAY GROUP BY 1),

  lista_real AS (SELECT DATE(datetime) day, idprocesoanterior, COUNT(1), SUM(total_cost) 
                 FROM `metrics-streams-dev`.`ProcessedData`.`BillingfacAllocationFinal` 
                 WHERE DATE(datetime) BETWEEN PARSE_DATE('%F', '2024-06-01') AND PARSE_DATE('%F', '2024-06-05')
                 GROUP BY 1,2 )
  
  SELECT * FROM matriz_days_pipes A WHERE EXISTS(SELECT 'X' FROM lista_validos C WHERE A.idproceso=C.idprocesoanterior)
                             AND NOT EXISTS(SELECT 'X' FROM lista_real B WHERE A.idproceso=B.idprocesoanterior AND A.days_gen=B.day)