{# Obtiene el registro mas reciente de los duplicados para posterior insercion #}

{{ config(
    materialized="table",
    dataset='TemporalData'
)}}


        WITH base_for_change AS (
          SELECT A.* FROM {{ ref('final_prefacIT')}} A 
          INNER JOIN {{ ref('final_dupli_check_it')}}  B 
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
