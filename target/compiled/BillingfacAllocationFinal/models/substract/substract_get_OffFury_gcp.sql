-- depends on : `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output` 









                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        





                      
        

                      
        

                      
        

                      
        

                      
        

                      
        

                      
        





                      
        

                      
        

                      
        


SELECT
      cast(TIMESTAMP_TRUNC(usage_start_time, DAY) AS datetime) datetime,
      CASE
        WHEN PROJECT.id IS NULL THEN 'Cargos no especificos de un proyecto'
        ELSE
          PROJECT.id
      END AS account_id,
      service.description AS service_type,
      sku.description AS billing_key,
      'NO-STACK' AS service_name,
      cast((SUM(CAST(cost * 1000000 AS INT64)) + SUM(IFNULL((
              SELECT
                SUM(CAST(c.amount * 1000000 AS INT64))
              FROM
                UNNEST(credits) c),
          0))) / 1000000 as numeric)  AS total_cost,
      usage.pricing_unit AS billing_unit,
      cast(SUM(usage.amount_in_pricing_units) as numeric)  AS billing_usage_amount,
      'GCP' AS provider
      ,current_date()  load_date	
      ,current_date() update_date
      ,'Test dbt' as observation 
    FROM `metrics-streams-dev`.`GoogleCloudBillingDetail`.`BillingDetailExport` 
    WHERE
      PARTITIONTIME >= DATETIME_SUB(CAST(DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) as TIMESTAMP), INTERVAL 1 DAY)
      AND PARTITIONTIME <= DATETIME_ADD(cast(DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) as TIMESTAMP ),INTERVAL 1 DAY)
      AND TIMESTAMP_TRUNC(usage_start_time, DAY) >=cast(DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) as timestamp)
      AND TIMESTAMP_TRUNC(usage_start_time, DAY) <= cast(DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) as timestamp)
       AND (   (  PROJECT.id = "gobiernoit-reservas" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "dev05-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-cp-bq02" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "dev04b-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-shp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bigqueue-test" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-mkt-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "dev02-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-fra-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-db-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "dev03-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bps-com-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "dev01-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "mid-mob-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbt-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "high-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-cp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpbs-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bigqueue" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "test-mp-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "low-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "bpt-api-bq01" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR  (  PROJECT.id = "shared-vpc-224614" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05"))  OR (resource.global_name like "%//bigquery.googleapis.com/projects/melidata-marketplace/datasets/melidata%" AND PROJECT.id = "melidata-marketplace" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (resource.name like "%desaenv%" AND PROJECT.id = "598925332888" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (resource.name like "%DESAENV%" AND PROJECT.id = "dbaas-268214" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (resource.name like "%desaenv%" AND PROJECT.id = "dbaas-268214" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (resource.name like "%DESAENV%" AND PROJECT.id = "598925332888" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (service.description = "Compute Engine" AND PROJECT.id = "fury-202121" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")) OR (service.description = "Compute Engine" AND PROJECT.id = "dbaas-268214" AND ( usage_start_time BETWEEN "2024-06-01" AND "2024-06-05")))
    GROUP BY 1, 2, 3, 4, 5, 7, 9, 10, 11, 12
    HAVING total_cost > 0.0