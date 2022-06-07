{% macro get_alerts_time_limit(days_back=31) %}
    {% set today = etl_utils.date_trunc('day', etl_utils.current_timestamp()) %}
    {% set datetime_limit = etl_utils.dateadd('day', days_back * -1, today) %}
    {{ return(datametry.cast_as_timestamp(datetime_limit)) }}
{% endmacro %}
