select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) =0 as should_error
    from (
      



WITH 
    full_proc AS (SELECT DISTINCT idprocesopadre as idproceso FROM `metrics-streams-dev`.`ManagementData`.`tproceso` ), 
    
        get_lista AS (SELECT CONCAT('[', STRING_AGG(CONCAT(CAST(idproceso AS STRING)), ','), ']') AS lista_in
                FROM ( SELECT DISTINCT idprocesopadre AS idproceso  FROM ManagementData.tproceso )),
      
    list_cut AS (SELECT REPLACE(REPLACE(REPLACE(lista_in,'[',''),']',''),' ','')  as lista_cut FROM get_lista ),
    proc_input AS (SELECT ARRAY_CONCAT(ARRAY_AGG(CAST(listado AS INT64)),[44]) array_process
                                FROM list_cut , UNNEST(SPLIT(lista_cut)) AS listado),
    partial_list AS (SELECT idproceso FROM proc_input , UNNEST(array_process) AS idproceso),                           
    proc_output AS (SELECT A.idproceso FROM partial_list A WHERE EXISTS(SELECT 'X' FROM full_proc B WHERE A.idproceso = B.idproceso))                            

    SELECT * FROM proc_output
      
    ) dbt_internal_test