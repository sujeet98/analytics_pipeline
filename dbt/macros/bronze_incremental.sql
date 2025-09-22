{% macro bronze_incremental(model_name, unique_key) %}
  {% set target_relation = this %}
  {% set src = ref(model_name) %}
  {% do return("""
    MERGE INTO {{ target_relation }} AS t
    USING (SELECT * FROM {{ src }}) s
    ON t.{{ unique_key }} = s.{{ unique_key }}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """) %}
{% endmacro %}
