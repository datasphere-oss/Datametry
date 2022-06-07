{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}

with alerts_schema_changes as (
    select * from {{ ref('datametry', 'alerts_schema_changes') }}
),

alerts_data_monitoring as (
    select * from {{ ref('datametry', 'alerts_data_monitoring') }}
),

alerts_dbt_tests as (
    select * from {{ ref('datametry', 'alerts_etl_tests') }}
),

all_alerts as (
     select * from alerts_schema_changes
     union all
     select * from alerts_data_monitoring
     union all
     select * from alerts_etl_tests
)

select *, false as alert_sent
from all_alerts
{%- if is_incremental() %}
{%- set row_count = datametry.get_row_count(this) %}
    {%- if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
