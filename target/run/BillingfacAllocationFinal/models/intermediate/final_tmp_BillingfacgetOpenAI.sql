
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetOpenAI`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`
SELECT 
          '199'  AS idcatalogoorigen,
          nombrenemonico,
          '63' AS idproceso,
          generator_cost_name, 
          account_id, 
          DATE(datetime) as datetime,
          service_name, 
          service_type, 
          billing_key, 
          billing_usage_ammount, 
          total_cost,  
          billing_concept, 
          billing_unit, 
          initiative_id, 
          initiative_external_code, 
          initiative_external_name, 
          head, 
          sponsor,
          project_id, 
          project_code, 
          team_id, 
          team_name, 
          avenue_id, 
          avenue_name, 
          bu_id,
          bu_name, 
          super_bu_id, 
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date, 
          'Test dbt' observation,
          CAST('151' AS INT64)  as idprocesoanterior
      FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabGENAI`  AS BillingtabGENAI
      WHERE DATE(datetime) >= DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY)
            AND DATE(datetime) <= DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
            AND EXISTS(SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` as tempprocesos WHERE 151=tempprocesos.idproceso)
    );
  