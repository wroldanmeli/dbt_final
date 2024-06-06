select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) >0 as should_error
    from (
      



 
 WITH lista_proc AS (
   SELECT lista_in FROM (SELECT  
        
         '[3, 56, 66, 151, 155, 29]' as lista_in)
      
   
 ) )

  

    SELECT * FROM lista_proc  WHERE 
        NOT REGEXP_CONTAINS(lista_in, '[0-9]') 
        OR NOT REGEXP_CONTAINS(lista_in, r'\[') 
        OR NOT REGEXP_CONTAINS(lista_in, r'\]')
        OR DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 5 DAY) > DATE(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), day, "UTC") - INTERVAL 1 DAY)
      
    ) dbt_internal_test