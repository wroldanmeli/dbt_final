-- depends_on: {{ ref('final_tmp_process_output') }}
{{ config(
    materialized="table",
    dataset='TemporalData'
)}}

        {%- call statement('get_snapshot_name', fetch_result=True) -%}
            SELECT REPLACE(REPLACE(REPLACE(CAST(DATETIME_TRUNC(CURRENT_DATETIME('UTC'), MINUTE) AS STRING),':','' ),'-',''),' ','' )
        {%- endcall -%}
        {%- set snapshot_name = load_result('get_snapshot_name')['data'][0][0] -%}

        {%- call statement('get_hour', fetch_result=True) -%}
            SELECT CAST(EXTRACT(HOUR FROM CURRENT_DATETIME('UTC')) AS STRING)
        {%- endcall -%}
        {%- set hour = load_result('get_hour')['data'][0][0] -%}

        {%- call statement('create_snapshot_final', fetch_result=False) -%}
                        EXECUTE IMMEDIATE format("""
                                CREATE SNAPSHOT TABLE ProcessedData_REPRO_snapshots.BillingfacAllocationFinal_%s CLONE ProcessedData.BillingfacAllocationFinal;""",'{{snapshot_name}}' );
        {%- endcall -%}

        {%- call statement('create_snapshot_baseoff', fetch_result=False) -%}
                      EXECUTE IMMEDIATE format("""
                                CREATE SNAPSHOT TABLE ProcessedData_REPRO_snapshots.BillingDashControlBaseOffFury_%s CLONE ProcessedData.BillingDashControlBaseOffFury;""", '{{snapshot_name}}');
        {%- endcall -%}

        {%- call statement('create_snapshot_base', fetch_result=False) -%}
                      EXECUTE IMMEDIATE format("""
                                CREATE SNAPSHOT TABLE ProcessedData_REPRO_snapshots.BillingDashControlBase_%s CLONE ProcessedData.BillingDashControlBase;""", '{{snapshot_name}}');        
        {%- endcall -%}
        

        {%- call statement('get_last_snapshot', fetch_result=True) -%}
                        SELECT table_id 
                        FROM ( SELECT CONCAT(dataset_id||'.'||table_id) table_id
                                    ,TIMESTAMP_MILLIS(last_modified_time) as last_modified_time           
                                FROM  ProcessedData_REPRO_snapshots.__TABLES__
                                WHERE table_id LIKE '%facAllocationFinal_%')
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY SUBSTR(table_id,1,25)
                        ORDER BY last_modified_time DESC) = 1;
        {%- endcall -%}

        {%- set last_snapshot = load_result('get_last_snapshot')['data'][0][0] -%}

        SELECT DATE(datetime) day_snap, '{{snapshot_name}}' sufijo , COUNT(1) records_snap, CAST(ROUND(SUM(total_cost),0) AS INT64) cost_snap 
        FROM {{last_snapshot}}  
        GROUP BY 1 

