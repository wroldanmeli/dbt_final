{# Recibe el parametro de entrada correspondiente al listado de procesos/pipelines  a ejecutar
   adiciona el id 44 de OffFury. En caso de venir vacia la lista, crea el listado completo por default  #}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{% set lista_str = var('v_list_pipelines') %}
WITH 
    get_lista AS (SELECT '{{ var("v_list_pipelines")}}' as lista_in),
    full_proc AS (SELECT DISTINCT idprocesopadre as idproceso FROM {{ source('ManagementData', 'tproceso') }} ),
    list_cut AS (SELECT REPLACE(REPLACE(REPLACE(lista_in,'[',''),']',''),' ','')  as lista_cut FROM get_lista ),
    proc_input AS (SELECT ARRAY_CONCAT(ARRAY_AGG(CAST(listado AS INT64)),[44]) array_process
                                FROM list_cut , UNNEST(SPLIT(lista_cut)) AS listado),
    partial_list AS (SELECT idproceso FROM proc_input , UNNEST(array_process) AS idproceso),                           
    proc_output AS (SELECT A.idproceso FROM partial_list A WHERE EXISTS(SELECT 'X' FROM full_proc B WHERE A.idproceso = B.idproceso))                            

    
    SELECT * FROM {% if lista_str == '[]' %} 
       full_proc
    {% else %}
       proc_output
    {% endif %}
