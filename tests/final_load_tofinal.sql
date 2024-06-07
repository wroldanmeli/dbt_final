WITH {# Validar que todos los pipelines seleccionados hayan movido datos a la final dentro
del rago de procesamiento#}

{{ config(
  enabled=true,
  error_if = '>1',
) }}
        {%- call statement('get_limit_date', fetch_result=True) -%}
            SELECT  '{{var('v_fecha_start')}}';
        {%- endcall -%}
        {%- set date_limit = load_result('get_limit_date')['data'][0][0] -%}

        {%- call statement('get_end_date', fetch_result=True) -%}
            SELECT  '{{var('v_fecha_end')}}';
        {%- endcall -%}
        {%- set date_end = load_result('get_end_date')['data'][0][0] -%}

  lista_proc AS (SELECT idproceso FROM {{ ref('final_tmp_process_output')}}),

  days_dummies AS (
        SELECT days_gen FROM 
        UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE('%F', '{{date_limit}}'), PARSE_DATE('%F', '{{date_limit}}')))  AS days_gen
        ),
  
  matriz_days_pipes AS (
       SELECT * 
       FROM days_dummies, lista_proc 
       ),


  lista_validos AS (SELECT idprocesoanterior FROM {{ ref('BillingfacAllocationFinal')}} 
                    WHERE DATE(datetime) BETWEEN PARSE_DATE('%F', '{{date_limit}}') - INTERVAL 7 DAY  
                          AND PARSE_DATE('%F', '{{date_limit}}') - INTERVAL 1 DAY GROUP BY 1),

  lista_real AS (SELECT DATE(datetime) day, idprocesoanterior, COUNT(1), SUM(total_cost) 
                 FROM {{ ref('BillingfacAllocationFinal')}} 
                 WHERE DATE(datetime) BETWEEN PARSE_DATE('%F', '{{date_limit}}') AND PARSE_DATE('%F', '{{date_end}}')
                 GROUP BY 1,2 )
  
  SELECT * FROM matriz_days_pipes A WHERE EXISTS(SELECT 'X' FROM lista_validos C WHERE A.idproceso=C.idprocesoanterior)
                             AND NOT EXISTS(SELECT 'X' FROM lista_real B WHERE A.idproceso=B.idprocesoanterior AND A.days_gen=B.day)
