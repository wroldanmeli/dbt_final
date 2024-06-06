
  
    

    create or replace table `metrics-streams-dev`.`billing_wr_test`.`final_prefacFI`
      
    
    

    OPTIONS()
    as (
      -- depends_on: __dbt__cte__final_get_finance_limits


WITH  __dbt__cte__final_get_finance_limits as (
-- depends_on: `metrics-streams-dev`.`TemporalData`.`final_prefacIT`





    
    


WITH get_current_day AS (
    SELECT CASE WHEN CAST('6' AS INT64) >= 20 THEN 
         DATE(DATE_TRUNC(CURRENT_DATE(), month) )
    ELSE
         DATE(DATE_TRUNC(CURRENT_DATE(), month) - INTERVAL 1 MONTH )
    END AS limit_finance),

    get_range AS (SELECT limit_finance, 
           DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) start_date, 
           DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY) end_date 
    FROM get_current_day), 

    get_limits_ind AS (
    SELECT 
        CASE 
            WHEN  A.start_date >= B.limit_finance AND A.end_date >= B.limit_finance THEN
                STRUCT(A.start_date as start_date, A.end_date as end_date) 
            WHEN  A.start_date < B.limit_finance AND A.end_date < B.limit_finance THEN
                STRUCT(DATE(CURRENT_DATE('UTC') + INTERVAL 100 DAY) as start_date, DATE(CURRENT_DATE('UTC') + INTERVAL 100 DAY) as end_date) 
            WHEN  A.start_date < B.limit_finance AND A.end_date >= B.limit_finance THEN
                STRUCT(B.limit_finance as start_date, DATE(A.end_date) as end_date ) 
       END AS output  
    FROM get_range A , get_current_day B )

    SELECT * FROM get_limits_ind
    WHERE output IS NOT NULL
), finance_limits AS (
    SELECT output.start_date start_date, 
           output.end_date end_date 
    FROM __dbt__cte__final_get_finance_limits
)

SELECT A.* FROM `metrics-streams-dev`.`TemporalData`.`final_prefacIT_nodup` A , finance_limits B
WHERE  DATE(A.datetime) BETWEEN B.start_date AND B.end_date
    );
  