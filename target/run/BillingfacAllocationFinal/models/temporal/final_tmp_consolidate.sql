
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate`
      
    
    

    OPTIONS()
    as (
      
-- depends on : (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetOffFury`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetFury`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetMelidata`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetBigquery`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetOpenAI`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetDatamesh`>, Undefined)


SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetOffFury`
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetFury`
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetMelidata`
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetBigquery`
UNION ALL
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetOpenAI`
UNION ALL
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingfacgetDatamesh`
UNION ALL
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_BillingtabMelidataBIFinalCosts`
    );
  