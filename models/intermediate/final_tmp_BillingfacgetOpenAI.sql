-- depends on : {{ ref('substract_all_in_one') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}
{%- set intidproceso = '63' -%}
{%- set idprocesoant = 151 -%}
{%- set idcatalogo = '199' -%}

SELECT 
          '{{idcatalogo}}'  AS idcatalogoorigen,
          nombrenemonico,
          '{{intidproceso}}' AS idproceso,
          generator_cost_name, 
          account_id, 
          DATE(datetime) as datetime,
          service_name, 
          service_type, 
          billing_key, 
          billing_usage_ammount, 
          total_cost,  
          billing_concept, 
          billing_unit, 
          initiative_id, 
          initiative_external_code, 
          initiative_external_name, 
          head, 
          sponsor,
          project_id, 
          project_code, 
          team_id, 
          team_name, 
          avenue_id, 
          avenue_name, 
          bu_id,
          bu_name, 
          super_bu_id, 
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date, 
          '{{var('v_strobservation')}}' observation,
          CAST('{{idprocesoant}}' AS INT64)  as idprocesoanterior
      FROM {{ source('ProcessedData', 'BillingtabGENAI') }}  AS BillingtabGENAI
      WHERE DATE(datetime) >= '{{var('v_fecha_start')}}'
            AND DATE(datetime) <= '{{var('v_fecha_end')}}'
            AND EXISTS(SELECT 'X' FROM {{ ref('final_tmp_process_output')}} as tempprocesos WHERE {{idprocesoant}}=tempprocesos.idproceso)
           