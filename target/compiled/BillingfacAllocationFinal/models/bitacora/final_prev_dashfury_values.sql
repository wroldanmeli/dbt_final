-- depends_on: `metrics-streams-dev`.`TemporalData`.`final_last_snapshot`
SELECT '20240606173900' sufijo, 
                PARSE_DATETIME('%Y%m%d%H%M%S', '20240606173900') datetime,
                COUNT(1) records,
                COALESCE(SUM(CAST(total_cost AS FLOAT64)),null,0) total_cost 
          FROM ProcessedData_REPRO_snapshots.BillingDashControlBase_20240606173900
          WHERE day BETWEEN DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) AND DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)