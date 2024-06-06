-- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into `metrics-streams-dev`.`billing_wr_test`.`BillingBitacoraRepro` as DBT_INTERNAL_DEST
        using (-- depends on : `metrics-streams-dev`.`TemporalData`.`final_new_record_bitacora`  



SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_new_record_bitacora`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.sufijo = DBT_INTERNAL_DEST.sufijo
                )

    
    when matched then update set
        `sufijo` = DBT_INTERNAL_SOURCE.`sufijo`,`datetime` = DBT_INTERNAL_SOURCE.`datetime`,`observation` = DBT_INTERNAL_SOURCE.`observation`,`last_final` = DBT_INTERNAL_SOURCE.`last_final`,`prev_final` = DBT_INTERNAL_SOURCE.`prev_final`,`last_dash_fury` = DBT_INTERNAL_SOURCE.`last_dash_fury`,`prev_dash_fury` = DBT_INTERNAL_SOURCE.`prev_dash_fury`,`last_dash_offfury` = DBT_INTERNAL_SOURCE.`last_dash_offfury`,`prev_dash_offfury` = DBT_INTERNAL_SOURCE.`prev_dash_offfury`,`dif_final_records` = DBT_INTERNAL_SOURCE.`dif_final_records`,`dif_final_cost` = DBT_INTERNAL_SOURCE.`dif_final_cost`,`dif_dash_fury_records` = DBT_INTERNAL_SOURCE.`dif_dash_fury_records`,`dif_dash_fury_cost` = DBT_INTERNAL_SOURCE.`dif_dash_fury_cost`,`dif_dash_offfury_records` = DBT_INTERNAL_SOURCE.`dif_dash_offfury_records`,`dif_dash_offfury_cost` = DBT_INTERNAL_SOURCE.`dif_dash_offfury_cost`,`date_start_repro` = DBT_INTERNAL_SOURCE.`date_start_repro`,`date_end_repro` = DBT_INTERNAL_SOURCE.`date_end_repro`,`struct_fury_dash` = DBT_INTERNAL_SOURCE.`struct_fury_dash`,`struct_offfury_dash` = DBT_INTERNAL_SOURCE.`struct_offfury_dash`
    

    when not matched then insert
        (`sufijo`, `datetime`, `observation`, `last_final`, `prev_final`, `last_dash_fury`, `prev_dash_fury`, `last_dash_offfury`, `prev_dash_offfury`, `dif_final_records`, `dif_final_cost`, `dif_dash_fury_records`, `dif_dash_fury_cost`, `dif_dash_offfury_records`, `dif_dash_offfury_cost`, `date_start_repro`, `date_end_repro`, `struct_fury_dash`, `struct_offfury_dash`)
    values
        (`sufijo`, `datetime`, `observation`, `last_final`, `prev_final`, `last_dash_fury`, `prev_dash_fury`, `last_dash_offfury`, `prev_dash_offfury`, `dif_final_records`, `dif_final_cost`, `dif_dash_fury_records`, `dif_dash_fury_cost`, `dif_dash_offfury_records`, `dif_dash_offfury_cost`, `date_start_repro`, `date_end_repro`, `struct_fury_dash`, `struct_offfury_dash`)


    