-- depends on : (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_aws`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_gcp`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_oci`>) 


SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_aws` 
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_gcp`
UNION ALL 
SELECT * FROM `metrics-streams-dev`.`TemporalData`.`substract_get_OffFury_oci`