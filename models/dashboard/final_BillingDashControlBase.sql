{{ config(
    materialized="table",
    dataset='ProcessedData',
    alias='BillingDashControlBase'
) }}

WITH 
    fecha_tablas AS (
        SELECT table_id,
               TIMESTAMP_MILLIS(last_modified_time) DateTIme_lastUpd, 
               row_count Count_rows, 
               size_bytes
        FROM {{ source('ProcessedData', '__TABLES__') }} ),

    days_dummies AS (
        SELECT days_gen FROM 
        UNNEST(GENERATE_DATE_ARRAY({{ var("v_fecha_start_dash") }}, DATE(CURRENT_DATE() - INTERVAL 2 DAY) , INTERVAL 1 DAY))  AS days_gen
        ),

    pipelines_dummies AS (
       SELECT idproceso, 
              proceso,
              Responsable 
       FROM ManagementData.tproceso 
       WHERE idproceso in ( 
              SELECT DISTINCT idproceso FROM {{ source('ProcessedData', 'BillingtabFuryAllocation') }} WHERE idproceso NOT IN (88) 
              UNION ALL 
              SELECT 29 )),

    pipes_day_real AS (
       SELECT DATE(B.datetime) dia,
                   B.idproceso, 
                   COUNT(1) conteo ,
                   SUM(B.total_cost) total_cost
       FROM {{ source('ProcessedData', 'BillingtabFuryAllocation') }} B 
       INNER JOIN pipelines_dummies C
             ON(B.idproceso=C.idproceso)
       GROUP BY 1,2 
       UNION ALL
       SELECT DATE(D.datetime) dia,
                   29 idproceso, 
                   COUNT(1) conteo ,
                   SUM(D.total_cost) total_cost
       FROM {{ source('ProcessedData', 'BillingtabMelidata') }} D
       GROUP BY 1,2
       UNION ALL
       SELECT DATE(E.datetime) dia,
                   155 idproceso, 
                   COUNT(1) conteo ,
                   SUM(E.total_cost) total_cost
       FROM  {{ source('ProcessedData', 'BillingtabBQMeliBIData') }} E
       GROUP BY 1,2 
       UNION ALL
       SELECT DATE(F.datetime) dia,
                   155 idproceso, 
                   COUNT(1) conteo ,
                   SUM(F.total_cost) total_cost
       FROM  {{ source('ProcessedData', 'BillingtabGENAI') }} F
       GROUP BY 1,2 
       UNION ALL
       SELECT DATE(G.datetime) dia,
                   155 idproceso, 
                   COUNT(1) conteo ,
                   SUM(G.total_cost) total_cost
       FROM  {{ source('ProcessedData', 'BillingtabMelidataBIFinalCosts') }} G
       GROUP BY 1,2 
       ),

    cruce_pipes_values AS (
       SELECT A.*,
              B.dia,
              B.conteo,
              B.total_cost 
       FROM pipelines_dummies A  
       LEFT JOIN pipes_day_real B 
            ON (A.idproceso=B.idproceso) 
       ),

    matriz_days_pipes AS (
       SELECT * 
       FROM days_dummies, pipelines_dummies
       ),

    selected_base AS (
       SELECT A.days_gen day, 
              A.idproceso,
              A.proceso,
              A.responsable,
              ROUND(COALESCE(B.conteo,null,0),0) conteo,
              ROUND(COALESCE(B.total_cost,null,0),2) total_cost 
       FROM  matriz_days_pipes A  
       LEFT JOIN cruce_pipes_values B 
            ON (A.days_gen = B.dia AND A.idproceso=B.idproceso) 
       ) 

    SELECT A.*,
      SUM(A.conteo)     OVER (PARTITION BY A.day ORDER BY day DESC) AS total_rows_day,
      SUM(A.total_cost) OVER (PARTITION BY A.day ORDER BY day DESC) AS total_cost_day,
      AVG(A.conteo)     OVER (PARTITION BY DATE(DATE_TRUNC(A.day , WEEK)), idproceso ) AS avg_rows_week,
      AVG(A.total_cost) OVER (PARTITION BY DATE(DATE_TRUNC(A.day , WEEK)), idproceso ) AS avg_cost_week,
      AVG(A.conteo)     OVER (PARTITION BY DATE(DATE_TRUNC(A.day , MONTH)), idproceso ) AS avg_rows_month,
      AVG(A.total_cost) OVER (PARTITION BY DATE(DATE_TRUNC(A.day , MONTH)),idproceso ) AS avg_cost_month 
   FROM selected_base A 