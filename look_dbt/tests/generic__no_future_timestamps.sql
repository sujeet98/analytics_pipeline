{% test no_future_timestamps(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} > current_timestamp()
{% endtest %}
