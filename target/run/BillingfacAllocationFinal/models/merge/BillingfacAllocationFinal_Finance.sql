-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `metrics-streams-dev`.`ProcessedData`.`BillingfacAllocationFinal_Finance` as DBT_INTERNAL_DEST
        using (

SELECT * FROM `metrics-streams-dev`.`billing_wr_test`.`final_prefacFI`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.datetime = DBT_INTERNAL_DEST.datetime
                ) and (
                    DBT_INTERNAL_SOURCE.generator_cost_name = DBT_INTERNAL_DEST.generator_cost_name
                ) and (
                    DBT_INTERNAL_SOURCE.account_id = DBT_INTERNAL_DEST.account_id
                ) and (
                    DBT_INTERNAL_SOURCE.service_type = DBT_INTERNAL_DEST.service_type
                ) and (
                    DBT_INTERNAL_SOURCE.billing_key = DBT_INTERNAL_DEST.billing_key
                ) and (
                    DBT_INTERNAL_SOURCE.service_name = DBT_INTERNAL_DEST.service_name
                ) and (
                    DBT_INTERNAL_SOURCE.billing_unit = DBT_INTERNAL_DEST.billing_unit
                ) and (
                    DBT_INTERNAL_SOURCE.billing_concept = DBT_INTERNAL_DEST.billing_concept
                ) and (
                    DBT_INTERNAL_SOURCE.idprocesoanterior = DBT_INTERNAL_DEST.idprocesoanterior
                )

    
    when matched then update set
        `idcatalogoorigen` = DBT_INTERNAL_SOURCE.`idcatalogoorigen`,`nombrenemonico` = DBT_INTERNAL_SOURCE.`nombrenemonico`,`idproceso` = DBT_INTERNAL_SOURCE.`idproceso`,`generator_cost_name` = DBT_INTERNAL_SOURCE.`generator_cost_name`,`account_id` = DBT_INTERNAL_SOURCE.`account_id`,`datetime` = DBT_INTERNAL_SOURCE.`datetime`,`service_name` = DBT_INTERNAL_SOURCE.`service_name`,`service_type` = DBT_INTERNAL_SOURCE.`service_type`,`billing_key` = DBT_INTERNAL_SOURCE.`billing_key`,`billing_usage_ammount` = DBT_INTERNAL_SOURCE.`billing_usage_ammount`,`total_cost` = DBT_INTERNAL_SOURCE.`total_cost`,`billing_concept` = DBT_INTERNAL_SOURCE.`billing_concept`,`billing_unit` = DBT_INTERNAL_SOURCE.`billing_unit`,`initiative_id` = DBT_INTERNAL_SOURCE.`initiative_id`,`initiative_external_code` = DBT_INTERNAL_SOURCE.`initiative_external_code`,`initiative_external_name` = DBT_INTERNAL_SOURCE.`initiative_external_name`,`head` = DBT_INTERNAL_SOURCE.`head`,`sponsor` = DBT_INTERNAL_SOURCE.`sponsor`,`project_id` = DBT_INTERNAL_SOURCE.`project_id`,`project_code` = DBT_INTERNAL_SOURCE.`project_code`,`team_id` = DBT_INTERNAL_SOURCE.`team_id`,`team_name` = DBT_INTERNAL_SOURCE.`team_name`,`avenue_id` = DBT_INTERNAL_SOURCE.`avenue_id`,`avenue_name` = DBT_INTERNAL_SOURCE.`avenue_name`,`bu_id` = DBT_INTERNAL_SOURCE.`bu_id`,`bu_name` = DBT_INTERNAL_SOURCE.`bu_name`,`super_bu_id` = DBT_INTERNAL_SOURCE.`super_bu_id`,`super_bu_name` = DBT_INTERNAL_SOURCE.`super_bu_name`,`load_date` = DBT_INTERNAL_SOURCE.`load_date`,`update_date` = DBT_INTERNAL_SOURCE.`update_date`,`observation` = DBT_INTERNAL_SOURCE.`observation`,`idprocesoanterior` = DBT_INTERNAL_SOURCE.`idprocesoanterior`,`consumption_type` = DBT_INTERNAL_SOURCE.`consumption_type`
    

    when not matched then insert
        (`idcatalogoorigen`, `nombrenemonico`, `idproceso`, `generator_cost_name`, `account_id`, `datetime`, `service_name`, `service_type`, `billing_key`, `billing_usage_ammount`, `total_cost`, `billing_concept`, `billing_unit`, `initiative_id`, `initiative_external_code`, `initiative_external_name`, `head`, `sponsor`, `project_id`, `project_code`, `team_id`, `team_name`, `avenue_id`, `avenue_name`, `bu_id`, `bu_name`, `super_bu_id`, `super_bu_name`, `load_date`, `update_date`, `observation`, `idprocesoanterior`, `consumption_type`)
    values
        (`idcatalogoorigen`, `nombrenemonico`, `idproceso`, `generator_cost_name`, `account_id`, `datetime`, `service_name`, `service_type`, `billing_key`, `billing_usage_ammount`, `total_cost`, `billing_concept`, `billing_unit`, `initiative_id`, `initiative_external_code`, `initiative_external_name`, `head`, `sponsor`, `project_id`, `project_code`, `team_id`, `team_name`, `avenue_id`, `avenue_name`, `bu_id`, `bu_name`, `super_bu_id`, `super_bu_name`, `load_date`, `update_date`, `observation`, `idprocesoanterior`, `consumption_type`)


    