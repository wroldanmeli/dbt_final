
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetDatamesh`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`
SELECT 
          '247'  AS idcatalogoorigen,
          'Bigquery Datamesh' AS nombrenemonico,
          '63'  AS idproceso,
          COALESCE(service_name,null,'No owner') AS generator_cost_name,
          'No Aplica' AS account_id, 
          DATE(datetime) as datetime,
          service_name, 
          service_type, 
          billing_key, 
          billing_usage_amount billing_usage_ammount, 
          total_cost,  
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
          CAST('155' AS INT64) as idprocesoanterior
      FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabBQDatamesh`  AS BillingtabBQDatamesh
      WHERE DATE(datetime) >= '2024-06-01'
            AND DATE(datetime) <= '2024-06-04'
            AND EXISTS(SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` as tempprocesos WHERE 155=tempprocesos.idproceso)
    );
  