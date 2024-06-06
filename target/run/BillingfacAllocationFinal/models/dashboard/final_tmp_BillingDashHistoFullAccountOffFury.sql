
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingDashHistoFullAccountOffFury`
      
    
    

    OPTIONS()
    as (
      -- depends_on: `metrics-streams-dev`.`ProcessedData`.`BillingfacAllocationFinal`



   WITH 
    days_dummies AS (
        SELECT days_gen FROM 
        UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE('%F', '2022-01-01'), DATE(CURRENT_DATE() - INTERVAL 2 DAY) , INTERVAL 1 DAY))  AS days_gen
        ),

    account_day_real AS (
       SELECT DATE(datetime) day,
            account_id, 
            account_name,
            idproceso,
            ROUND(SUM(total_cost),2) total_cost,
            COUNT(1) conteo
       FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFury` B 
       WHERE account_id IS NOT NULL AND account_name IS NOT NULL
       GROUP BY 1,2,3,4
       ),

    account_dummies AS (
       SELECT account_id, account_name, idproceso
       FROM account_day_real 
       WHERE account_name IS NOT NULL AND account_id IS NOT NULL
       GROUP BY 1,2,3 
       ),

    matriz_days_account AS (
       SELECT A.days_gen, B.account_id , B.account_name, B.idproceso
       FROM days_dummies A, account_dummies B
       ),

    cruce_account_values AS (
       SELECT A.days_gen day,
              A.account_id,
              A.account_name,
              A.idproceso,
              COALESCE(B.total_cost,null,0) total_cost,
              COALESCE(B.conteo,null,0) conteo
       FROM matriz_days_account A
       LEFT JOIN account_day_real B
            ON (A.days_gen=B.day AND A.account_id=B.account_id AND A.account_name=B.account_name AND A.idproceso=B.idproceso)
       )

    SELECT * FROM cruce_account_values
    );
  