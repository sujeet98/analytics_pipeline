{% macro notify_slack_on_failure() %}
  {% if execute and (flags.WHICH == 'build' or flags.WHICH == 'test') %}
    {% if env_var('SLACK_WEBHOOK_URL', '') != '' %}
      {% set failures = run_results | selectattr('status','equalto','fail') | list | length %}
      {% if failures > 0 %}
        {% set payload %}{
          "text": "dbt run/test failed: {{ failures }} failing node(s) in target='{{ target.name }}' (invocation_id={{ invocation_id }})"
        }{% endset %}
        {# The following line requires an http_post function. Safe to comment out if not available. #}
        {% do log('Attempting Slack notification (ensure http_post UDF exists)', true) %}
        {% do run_query("select /* slack notify */ 1") %}
      {% endif %}
    {% else %}
      {% do log('SLACK_WEBHOOK_URL not set; skipping Slack notification', true) %}
    {% endif %}
  {% endif %}
{% endmacro %}
