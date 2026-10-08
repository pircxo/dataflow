{#
  By default dbt names custom schemas "<target_schema>_<custom>", e.g. "staging_analytics".
  We want exactly "staging" and "analytics", so use the custom name as-is.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
