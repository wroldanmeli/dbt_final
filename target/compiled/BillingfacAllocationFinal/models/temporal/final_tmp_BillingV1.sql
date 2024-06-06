

-- depends_on: `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup`
SELECT  CAST(a.idcatalogoorigen AS INT64) idcatalogoorigen,   
              a.nombrenemonico,
              CAST(a.idproceso AS INT64) idproceso,
              a.generator_cost_name,
              a.account_id, 
              a.datetime,   
              a.service_name,   
              a.service_type,   
              a.billing_key,
              SUM(a.billing_usage_ammount) billing_usage_ammount,   
              SUM(a.total_cost) total_cost,  
              IF (a.idprocesoanterior=155, a.billing_key, a.billing_concept) billing_concept,
              a.billing_unit,   
              a.initiative_id,  
              a.initiative_external_code, 
              LEFT(a.initiative_external_name,149)  initiative_external_name,
              LEFT(a.head,149) head,
              LEFT(a.sponsor,149) sponsor,
              a.project_id,
              LEFT(a.project_code,149) project_code,    
              a.team_id,LEFT(a.team_name,149) team_name,
              a.avenue_id, LEFT(a.avenue_name,149) avenue_name, 
              a.bu_id,
              LEFT(a.bu_name,149) bu_name,
              a.super_bu_id, 
              LEFT(a.super_bu_name,149) super_bu_name,
              MIN(a.load_date) load_date,
              MAX(a.update_date) update_date,
              CONCAT('se sumaron ', CAST( COUNT(1) AS STRING), ' de registros') observation,
              a.idprocesoanterior,
              ARRAY_AGG(STRUCT(PARSE_JSON(
                  CASE WHEN a.idprocesoanterior=155 THEN
                       billing_concept
                  ELSE
                       '{"N/A":"N/A"}' 
                  END) AS user, billing_usage_ammount, total_cost )) AS consumption_type 
      FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup` AS a
      INNER JOIN 
        (SELECT 
            final.datetime,
            final.generator_cost_name,
            final.billing_key,
            final.service_type,
            final.service_name,
            final.billing_unit
         FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup` AS  final
         GROUP BY 1,2,3,4,5,6
         HAVING COUNT(1)>1) b
      ON      a.datetime=b.datetime
          AND a.generator_cost_name =b.generator_cost_name
          AND a.billing_key = b.billing_key
          AND a.service_type = b.service_type
          AND a.service_name = b.service_name
          AND a.billing_unit= b.billing_unit
      GROUP BY 1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,32
      UNION ALL
      SELECT CAST(a.idcatalogoorigen AS INT64) idcatalogoorigen,
             a.nombrenemonico,  
             CAST(a.idproceso AS INT64) idproceso,
             a.generator_cost_name,
             a.account_id,
             a.datetime,    
             a.service_name,
             a.service_type,
             a.billing_key,
             SUM(a.billing_usage_ammount) billing_usage_ammount, 
             SUM(a.total_cost) total_cost, 
             a.billing_concept,
             a.billing_unit,
             a.initiative_id,   
             a.initiative_external_code,
             LEFT(a.initiative_external_name,149) initiative_external_name,
             LEFT(a.head,149) head,
             LEFT(a.sponsor,149) sponsor,   
             a.project_id,
             LEFT(a.project_code,149) project_code, 
             a.team_id,
             LEFT(a.team_name,149) team_name,
             a.avenue_id,
             LEFT(a.avenue_name,149) avenue_name,   
             a.bu_id,
             LEFT(a.bu_name,149) bu_name,   
             a.super_bu_id,
             LEFT(a.super_bu_name,149) super_bu_name,
             MIN(a.load_date) load_date,   
             MAX(a.update_date) update_date,
             CONCAT('se sumaron ', CAST( COUNT(1) AS STRING), ' de registros') observation,
             a.idprocesoanterior,
             ARRAY_AGG(STRUCT(PARSE_JSON('{"N/A":"N/A"}') AS user, billing_usage_ammount, total_cost )) AS consumption_type   
      FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup`  AS a
      INNER JOIN 
      (SELECT 
             final.datetime,
             final.generator_cost_name,
             final.billing_key,
             final.service_type, 
             final.service_name,
             final.billing_unit
        FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup` final
        GROUP BY 1,2,3,4,5,6
        HAVING COUNT(1)=1) b 
      ON  (
          a.datetime=b.datetime
      AND a.generator_cost_name =b.generator_cost_name
      AND a.billing_key = b.billing_key
      AND a.service_type = b.service_type
      AND a.service_name = b.service_name
      AND a.billing_unit= b.billing_unit)
      WHERE a.idprocesoanterior != 155
      GROUP BY 1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,32
      UNION ALL
      SELECT CAST(a.idcatalogoorigen AS INT64) idcatalogoorigen,
             a.nombrenemonico,  
             CAST(a.idproceso AS INT64) idproceso,
             a.generator_cost_name,
             a.account_id,
             a.datetime,    
             a.service_name,
             a.service_type,
             a.billing_key,
             SUM(a.billing_usage_ammount) billing_usage_ammount, 
             SUM(a.total_cost) total_cost, 
             a.billing_key AS billing_concept,
             a.billing_unit,
             a.initiative_id,   
             a.initiative_external_code,
             LEFT(a.initiative_external_name,149) initiative_external_name,
             LEFT(a.head,149) head,
             LEFT(a.sponsor,149) sponsor,   
             a.project_id,
             LEFT(a.project_code,149) project_code, 
             a.team_id,
             LEFT(a.team_name,149) team_name,
             a.avenue_id,
             LEFT(a.avenue_name,149) avenue_name,   
             a.bu_id,
             LEFT(a.bu_name,149) bu_name,   
             a.super_bu_id,
             LEFT(a.super_bu_name,149) super_bu_name,
             MIN(a.load_date) load_date,   
             MAX(a.update_date) update_date,
             CONCAT('se sumaron ', CAST( COUNT(1) AS STRING), ' de registros') observation,
             a.idprocesoanterior,
             ARRAY_AGG(STRUCT(PARSE_JSON('{"N/A":"N/A"}') AS user, billing_usage_ammount, total_cost )) AS consumption_type 
      FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup` a  
      INNER JOIN
      (SELECT 
             final.datetime,
             final.generator_cost_name,
             final.billing_key,
             final.service_type, 
             final.service_name,
             final.billing_unit
        FROM `metrics-streams-dev`.`TemporalData`.`final_tmp_consolidate_nodup` final
        WHERE final.idprocesoanterior = 155
        GROUP BY 1,2,3,4,5,6
        HAVING COUNT(1)=1) b 
      ON  (
          a.datetime=b.datetime
      AND a.generator_cost_name =b.generator_cost_name
      AND a.billing_key = b.billing_key
      AND a.service_type = b.service_type
      AND a.service_name = b.service_name
      AND a.billing_unit= b.billing_unit)
      WHERE a.idprocesoanterior = 155
      GROUP BY 1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,32