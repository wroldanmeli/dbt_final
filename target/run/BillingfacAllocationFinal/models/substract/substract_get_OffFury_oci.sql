
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_oci`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` 








       
           

       
           

       
           

       
           

       
           

       
           

       
           


SELECT
      cast(TIMESTAMP_TRUNC(cost_lineItem_intervalUsageStart, DAY) AS datetime) datetime,
     'ocid1.tenancy.oc1..aaaaaaaalgfveinmsacv4lo36wk4hfoiokh2m52j72sw5f4boms676vvc2da' AS account_id,
      CONCAT('OCI_',cost_product_Description) AS service_type,
      COALESCE(usage_product_resource,null,'UNDEFINED')  AS billing_key,
      'NO-STACK' AS service_name,
      CAST(SUM(ROUND(cost_total_cost_US,2)) AS NUMERIC)  AS total_cost,
      COALESCE(usage_consumedQuantityUnits,null,'UNDEFINED') billing_unit,
      CAST(SUM(ROUND(cost_total_usage,2)) AS NUMERIC)  AS billing_usage_amount,
      'OCI' AS provider
      ,current_date()  load_date
      ,current_date() update_date
      ,'Test dbt' as observation
    FROM `metrics-streams-dev`.`WorkData`.`BillingOracleAnnualUnified`
    WHERE
        (cost_product_Description = "Exadata X9M Additional Storage Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Exadata X8M Additional Storage Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Exadata X9M Additional Compute Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Database Exadata - Additional OCPUs - BYOL"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Exadata X9M Quarter Rack Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Exadata X8M Quarter Rack Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04") OR (cost_product_Description = "Exadata X8M Additional Compute Instance Meter"  AND  cost_lineItem_intervalUsageStart BETWEEN "2024-06-01" AND "2024-06-04")
    GROUP BY 1,2,3,4,5,7,9,10,11,12
    HAVING total_cost > 0.0
    );
  