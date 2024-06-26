
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingtabMelidataBIFinalCosts`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`
SELECT 
          '248'  AS idcatalogoorigen,
          'melidata-storage' AS nombrenemonico,
          '63'  AS idproceso,
          application_name AS generator_cost_name,
          'No Aplica' AS account_id, 
          DATE(datetime) datetime,
          service_name, 
          service_type, 
          billing_key, 
          CAST(billing_usage_amount AS numeric) billing_usage_amount, 
          CAST(total_cost AS numeric) total_cost,  
          billing_concept, 
          billing_unit, 
          initiative_id, 
          initiative_external_code, 
          initiative_external_name, 
          head,
          sponsor,
          1 project_id, 
          'No aplica' project_code, 
          1 team_id, 
          'No aplica' team_name, 
          avenue_id, 
          avenue_name, 
          bu_id,bu_name, 
          super_bu_id, 
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date, 
          'Test Final dbt' observation,
          210 AS idprocesoanterior
FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabMelidataBIFinalCosts`  AS BillingtabMelidata
      WHERE DATE(datetime) >= '2024-06-01'
            AND DATE(datetime) <= '2024-06-04'
            AND EXISTS(SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` as tempprocesos WHERE 210=tempprocesos.idproceso)
    );
  