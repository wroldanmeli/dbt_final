
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_consolidate_nodup`
      
    
    

    OPTIONS()
    as (
      



WITH cut_duplis AS (SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate`  A 
                    WHERE NOT EXISTS (SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`final_dupli_check_it_reinsert_it` B WHERE
                        A.datetime=B.datetime AND
                        A.generator_cost_name = B.generator_cost_name AND
                        A.account_id = B.account_id AND 
                        A.service_type = B.service_type AND
                        A.billing_key = B.billing_key AND
                        A.service_name = B.service_name AND  
                        A.billing_unit = B.billing_unit AND 
                        A.billing_concept = B.billing_concept AND 
                        A.idprocesoanterior = B.idprocesoanterior) AND A.initiative_id IS NOT NULL
                    )

SELECT * FROM cut_duplis  
UNION ALL
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_dupli_check_it_reinsert_it`
    );
  