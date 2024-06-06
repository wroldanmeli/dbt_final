-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `metrics-streams-dev`.`ProcessedData`.`BillingtabOffFurysubtract` as DBT_INTERNAL_DEST
        using (-- depends on : (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_aws`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_gcp`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_oci`>) 


SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_aws` 
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_gcp`
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_oci`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.datetime = DBT_INTERNAL_DEST.datetime
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
                    DBT_INTERNAL_SOURCE.provider = DBT_INTERNAL_DEST.provider
                )

    
    when matched then update set
        `datetime` = DBT_INTERNAL_SOURCE.`datetime`,`account_id` = DBT_INTERNAL_SOURCE.`account_id`,`service_type` = DBT_INTERNAL_SOURCE.`service_type`,`billing_key` = DBT_INTERNAL_SOURCE.`billing_key`,`service_name` = DBT_INTERNAL_SOURCE.`service_name`,`total_cost` = DBT_INTERNAL_SOURCE.`total_cost`,`billing_unit` = DBT_INTERNAL_SOURCE.`billing_unit`,`billing_usage_ammount` = DBT_INTERNAL_SOURCE.`billing_usage_ammount`,`provider` = DBT_INTERNAL_SOURCE.`provider`,`load_date` = DBT_INTERNAL_SOURCE.`load_date`,`update_date` = DBT_INTERNAL_SOURCE.`update_date`,`observation` = DBT_INTERNAL_SOURCE.`observation`
    

    when not matched then insert
        (`datetime`, `account_id`, `service_type`, `billing_key`, `service_name`, `total_cost`, `billing_unit`, `billing_usage_ammount`, `provider`, `load_date`, `update_date`, `observation`)
    values
        (`datetime`, `account_id`, `service_type`, `billing_key`, `service_name`, `total_cost`, `billing_unit`, `billing_usage_ammount`, `provider`, `load_date`, `update_date`, `observation`)


    