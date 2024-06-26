-- depends on : {{ ref('substract_all_in_one') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

{%- set strorigen = 'OffFury' -%}
{%- set strprovider = 'aws' -%}
{%- set intidproceso = 63 -%}
{%- set idcatalogo = 72 -%} 

SELECT 
          '{{idcatalogo}}'  AS idcatalogoorigen,
          '{{strorigen}}' AS nombrenemonico,
          '{{intidproceso}}'  AS idproceso,
          account_name AS generator_cost_name,
          BillingtabOffFury.account_id,
          DATE(BillingtabOffFury.datetime) as datetime,
          BillingtabOffFury.service_name, 
          BillingtabOffFury.service_type, 
          BillingtabOffFury.billing_key, 
          ROUND((billing_usage_amount - IFNULL(billing_usage_ammount,0)),2) AS billing_usage_ammount,
          ROUND((BillingtabOffFury.total_cost - IFNULL(BillingtabOffFurysubtract.total_cost,0)),2) AS total_cost,
          billing_concept, 
          BillingtabOffFury.billing_unit, 
          CAST(initiative_id AS INT64) initiative_id,  
          initiative_external_code, 
          initiative_external_name, 
          head, 
          sponsor, 
          1 project_id, 
          'No Aplica' project_code, 
          1 team_id, 
          'No Aplica' team_name,
          CAST(avenue_id AS INT64) avenue_id , 
          avenue_name,
          CAST(bu_id AS INT64) bu_id,
          bu_name, 
          CAST(super_bu_id AS INT64) super_bu_id, 
          super_bu_name, 
          BillingtabOffFury.load_date, 
          CURRENT_DATETIME('UTC') AS update_date,
          '{{var('v_strobservation')}}' observation, 
          BillingtabOffFury.idproceso  as idprocesoanterior
      FROM {{ source('ProcessedData', 'BillingtabOffFury') }} as BillingtabOffFury
      LEFT JOIN {{ ref('substract_all_in_one') }} as BillingtabOffFurysubtract 
           ON (DATE(BillingtabOffFury.datetime) = DATE(BillingtabOffFurysubtract.datetime)
              AND BillingtabOffFury.account_id  =BillingtabOffFurysubtract.account_id
              AND BillingtabOffFury.service_type =BillingtabOffFurysubtract.service_type
              AND BillingtabOffFury.billing_key =BillingtabOffFurysubtract.billing_key
              AND BillingtabOffFury.service_name =BillingtabOffFurysubtract.service_name
              AND BillingtabOffFury.billing_unit =BillingtabOffFurysubtract.billing_unit)
      WHERE DATE(BillingtabOffFury.datetime) >= {{ get_start_date() }}
            AND DATE(BillingtabOffFury.datetime) <= {{ get_end_date() }} 
            AND (BillingtabOffFury.total_cost - IFNULL(BillingtabOffFurysubtract.total_cost,0)) > 0 
             AND EXISTS(SELECT 'X' FROM {{ ref('final_tmp_process_output')}} as tempprocesos WHERE BillingtabOffFury.idproceso=tempprocesos.idproceso)
           