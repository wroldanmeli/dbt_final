
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetFury`
      
    
    

    OPTIONS()
    as (
      -- depends on : `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract`
SELECT 
          '70'  AS idcatalogoorigen,
          'Fury' AS nombrenemonico,
          '63'  AS idproceso,
          application_name AS generator_cost_name,
          'No Aplica' AS account_id,
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
          bu_id,bu_name, 
          super_bu_id,  
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date,
          'Test dbt' observation,
          idproceso  as idprocesoanterior
      FROM `metrics-streams-dev`.`ProcessedData`.`BillingtabFuryAllocation` as BillingtabFury
      WHERE DATE(datetime) >= '2024-06-01'
            AND DATE(datetime) <= '2024-06-04'
            AND NOT EXISTS(SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`substract_get_rules_fury` as restarfury 
                           WHERE restarfury.tag_key='application_name' AND BillingtabFury.application_name=restarfury.tag_value )
            AND EXISTS(SELECT 'X' FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` as tempprocesos WHERE BillingtabFury.idproceso=tempprocesos.idproceso)
    );
  