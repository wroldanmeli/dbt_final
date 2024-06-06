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