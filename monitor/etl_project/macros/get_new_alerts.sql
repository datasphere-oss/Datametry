{% macro get_new_alerts(days_back, results_sample_limit = 5) %}
    -- depends_on: {{ ref('alerts') }}
    {% set current_date = etl_utils.date_trunc('day', etl_utils.current_timestamp()) %}
    {% set select_new_alerts_query %}
        SELECT alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type,
               alert_description, owners, tags, alert_results_query, other, test_name, test_params, severity, status
        FROM {{ ref('alerts') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit(days_back) }}
    {% endset %}
    {% set results = run_query(select_new_alerts_query) %}
    {% set new_alerts = [] %}
    {% for result in results %}
        {% set result_dict = result.dict() %}
        {% set alert_results_query = datametry.insensitive_get_dict_value(result_dict, 'alert_results_query') %}
        {% set alert_type = datametry.insensitive_get_dict_value(result_dict, 'alert_type') %}
        {% set status = datametry.insensitive_get_dict_value(result_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {% if alert_results_query and status != 'error' and alert_type == 'dbt_test' %}
            {% set alert_results_query_with_limit = alert_results_query ~ ' limit ' ~ results_sample_limit %}
            {% set test_results = run_query(alert_results_query_with_limit) %}
            {% set test_rows_sample = datametry.agate_to_json(test_results) %}
        {% endif %}

        {% set new_alert_dict = {'alert_id': datametry.insensitive_get_dict_value(result_dict, 'alert_id'),
                                 'detected_at': datametry.insensitive_get_dict_value(result_dict, 'detected_at').isoformat(),
                                 'database_name': datametry.insensitive_get_dict_value(result_dict, 'database_name'),
                                 'schema_name': datametry.insensitive_get_dict_value(result_dict, 'schema_name'),
                                 'table_name': datametry.insensitive_get_dict_value(result_dict, 'table_name'),
                                 'column_name': datametry.insensitive_get_dict_value(result_dict, 'column_name'),
                                 'alert_type': alert_type,
                                 'sub_type': datametry.insensitive_get_dict_value(result_dict, 'sub_type'),
                                 'alert_description': datametry.insensitive_get_dict_value(result_dict, 'alert_description'),
                                 'owners': datametry.insensitive_get_dict_value(result_dict, 'owners'),
                                 'tags': datametry.insensitive_get_dict_value(result_dict, 'tags'),
                                 'alert_results_query': alert_results_query,
                                 'alert_results': test_rows_sample,
                                 'other': datametry.insensitive_get_dict_value(result_dict, 'other'),
                                 'test_name': datametry.insensitive_get_dict_value(result_dict, 'test_name'),
                                 'test_params': datametry.insensitive_get_dict_value(result_dict, 'test_params'),
                                 'severity': datametry.insensitive_get_dict_value(result_dict, 'severity'),
                                 'status': status} %}
        {% set new_alert_json = tojson(new_alert_dict) %}
        {% do datametry.edr_log(new_alert_json) %}
    {% endfor %}
{% endmacro %}

