{# Small helpers to keep staging models DRY and readable. #}

{% macro nullif_blank(value) -%}
    /* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif({{ value }}, ''), '-')
{%- endmacro %}

{% macro trim_lower(value) -%}
    /* Lowercase + trim for consistent string comparisons/joins downstream */
    lower(trim({{ value }}))
{%- endmacro %}

{% macro to_timestamp_safe(value) -%}
    /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast({{ value }} AS timestamp)
{%- endmacro %}

{% macro to_double_safe(value) -%}
    /* Safe cast to double with NULL on failure */
    try_cast({{ value }} AS double)
{%- endmacro %}

{% macro to_bigint_safe(value) -%}
    /* Safe cast to bigint with NULL on failure */
    try_cast({{ value }} AS bigint)
{%- endmacro %}

{% macro coalesce_zero(value) -%}
    coalesce({{ value }}, 0)
{%- endmacro %}
