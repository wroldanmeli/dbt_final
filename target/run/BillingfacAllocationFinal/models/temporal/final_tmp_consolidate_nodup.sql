
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup`
      
    
    

    OPTIONS()
    as (
      



WITH  __dbt__cte__final_dupli_7fields_it as (



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
        FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate`
        GROUP BY 1,2,3,4,5,6,7,8,9
        HAVING COUNT(*) > 1
),  __dbt__cte__final_dups_key_7fields_for_insert_it as (





        WITH base_for_change AS (
          SELECT A.* FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate` A 
          INNER JOIN __dbt__cte__final_dupli_7fields_it  B 
          ON (DATE(A.datetime) = B.day AND
              A.generator_cost_name = B.generator_cost_name AND
              A.account_id = B.account_id AND 
              A.service_type = B.service_type AND
              A.billing_key = B.billing_key AND
              A.service_name = B.service_name AND  
              A.billing_unit = B.billing_unit AND 
              A.billing_concept = B.billing_concept AND 
              A.idprocesoanterior = B.idprocesoanterior)  
        )

        SELECT * FROM base_for_change 
        WHERE TRUE 
        QUALIFY ROW_NUMBER() 
        OVER (PARTITION BY 
                DATE(datetime),
                generator_cost_name,
                account_id,
                service_type,
                billing_key,
                service_name,
                billing_unit,
                billing_concept,
                idprocesoanterior
                ORDER BY load_date DESC) = 1
), cut_duplis AS (SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate`  A 
                    WHERE NOT EXISTS (SELECT 'X' FROM __dbt__cte__final_dups_key_7fields_for_insert_it B WHERE
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
SELECT * FROM __dbt__cte__final_dups_key_7fields_for_insert_it
    );
  