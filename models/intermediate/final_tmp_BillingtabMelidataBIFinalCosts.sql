-- depends on : {{ ref('substract_all_in_one') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'melidata-storage' -%}
{%- set intidproceso = '63' -%}
{%- set idcatalogo = '248' -%}
{%- set idprocesoant = 210 -%}

SELECT 
          '{{idcatalogo}}'  AS idcatalogoorigen,
          '{{strorigen}}' AS nombrenemonico,
          '{{intidproceso}}'  AS idproceso,
          application_name AS generator_cost_name,
          'No Aplica' AS account_id, 
          DATE(datetime) datetime,
          service_name, 
          service_type, 
          billing_key, 
          CAST(billing_usage_amount AS numeric) billing_usage_amount, 
          CAST(total_cost AS numeric) total_cost,  
          billing_concept, 
          billing_unit, 
          initiative_id, 
          initiative_external_code, 
          initiative_external_name, 
          head,
          sponsor,
          1 project_id, 
          'No aplica' project_code, 
          1 team_id, 
          'No aplica' team_name, 
          avenue_id, 
          avenue_name, 
          bu_id,bu_name, 
          super_bu_id, 
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date, 
          '{{var('v_strobservation')}}' observation,
          {{idprocesoant}} AS idprocesoanterior
FROM {{ source('ProcessedData', 'BillingtabMelidataBIFinalCosts') }}  AS BillingtabMelidata
      WHERE DATE(datetime) >= {{ get_start_date() }}
            AND DATE(datetime) <= {{ get_end_date() }}
            AND EXISTS(SELECT 'X' FROM {{ ref('final_tmp_process_output')}} as tempprocesos WHERE {{idprocesoant}}=tempprocesos.idproceso)
 