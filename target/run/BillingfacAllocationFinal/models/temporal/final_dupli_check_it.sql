
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_dupli_check_it`
      
    
    

    OPTIONS()
    as (
      


        SELECT DATE(datetime) day
              ,generator_cost_name
              ,account_id
              ,service_type  
              ,billing_key
              ,service_name
              ,billing_unit 
              ,billing_concept
              ,idprocesoanterior
              ,COUNT(1) records
              ,SUM(total_cost) total_cost
        FROM `metrics-streams-dev`.`TemporalData`.`final_prefacIT`
        GROUP BY 1,2,3,4,5,6,7,8,9
        HAVING COUNT(*) > 1
    );
  