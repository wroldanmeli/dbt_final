-- depends on : {{ ref('substract_all_in_one') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'Fury' -%}
{%- set strprovider = 'Fury' -%}
{%- set intidproceso = 63 -%}
{%- set idcatalogo = 70 -%}

SELECT 
          '{{idcatalogo}}'  AS idcatalogoorigen,
          '{{strorigen}}' AS nombrenemonico,
          '{{intidproceso}}'  AS idproceso,
          application_name AS generator_cost_name,
          'No Aplica' AS account_id,
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
          bu_id,bu_name, 
          super_bu_id,  
          super_bu_name, 
          load_date, 
          CURRENT_DATETIME('UTC') AS update_date,
          '{{var('v_strobservation')}}' observation,
          idproceso  as idprocesoanterior
      FROM {{ source('ProcessedData', 'BillingtabFuryAllocation') }} as BillingtabFury
      WHERE DATE(datetime) >= {{ get_start_date() }}
            AND DATE(datetime) <= {{ get_end_date() }}
            AND NOT EXISTS(SELECT 'X' FROM {{ ref('substract_get_rules_fury')}} as restarfury 
                           WHERE restarfury.tag_key='application_name' AND BillingtabFury.application_name=restarfury.tag_value )
            AND EXISTS(SELECT 'X' FROM {{ ref('final_tmp_process_output')}} as tempprocesos WHERE BillingtabFury.idproceso=tempprocesos.idproceso)
           