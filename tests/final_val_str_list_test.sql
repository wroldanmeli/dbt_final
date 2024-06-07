{# Validar que la lista de idprocesos este bien conformada con valores separados por coma y entre parentesis cuadrados 
   verifica si la lista es completa [] o viene seleccionada con algunos id pipelines #}
{{ config(
         error_if = '>0',
         enabled=false)
 }}

{% set lista_str = var('v_list_pipelines') %}
 
 WITH lista_proc AS (
   SELECT lista_in FROM (SELECT  
       {% if lista_str == '[]' %}
        CONCAT('[', STRING_AGG(CONCAT(CAST(idproceso AS STRING)), ','), ']') AS lista_in
                FROM (SELECT DISTINCT idprocesopadre as idproceso FROM {{ source('ManagementData', 'tproceso') }})
    {% else %} 
         '{{ var("v_list_pipelines")}}' as lista_in)
    {% endif %}  
   
 ) )

  

    SELECT * FROM lista_proc  WHERE 
        NOT REGEXP_CONTAINS(lista_in, '[0-9]') 
        OR NOT REGEXP_CONTAINS(lista_in, r'\[') 
        OR NOT REGEXP_CONTAINS(lista_in, r'\]')
        OR '{{var('v_fecha_start')}}' > '{{var('v_fecha_end')}}' 