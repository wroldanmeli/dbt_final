
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`BillingDashHistoFullSubstract`
      
    
    

    OPTIONS()
    as (
      

   WITH 
    days_dummies AS (
        SELECT days_gen FROM 
        UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE('%F', '2022-01-01'), DATE(CURRENT_DATE() - INTERVAL 2 DAY) , INTERVAL 1 DAY))  AS days_gen
        ),

   only_real_service_type AS (
        SELECT DISTINCT service_type 
        FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`  
   ),

    service_type_day_real AS (
       SELECT DATE(datetime) day,
            service_type,
            ROUND(SUM(total_cost),2) total_cost,
            COUNT(1) conteo
       FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`  
       WHERE service_type IS NOT NULL
       GROUP BY 1,2
       ),

    service_type_dummies AS (
       SELECT DISTINCT service_type
       FROM only_real_service_type 
       WHERE service_type IS NOT NULL
       ),

    matriz_days_service_type AS (
       SELECT A.days_gen, B.service_type 
       FROM days_dummies A, service_type_dummies B
       ),

    cruce_service_type_values AS (
       SELECT A.days_gen day,
              A.service_type,
              COALESCE(B.total_cost,null,0) total_cost,
              COALESCE(B.conteo,null,0) conteo
       FROM matriz_days_service_type A
       LEFT JOIN service_type_day_real B
            ON (A.days_gen=B.day AND A.service_type=B.service_type)
       )

    SELECT * FROM cruce_service_type_values
    );
  