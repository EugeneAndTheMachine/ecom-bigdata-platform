{#- Returns the relation if it exists, else none (lets Gold build before ML ran once). -#}
{% macro optional_relation(schema, identifier) -%}
  {%- if execute -%}
    {{ return(adapter.get_relation(database=target.database, schema=schema, identifier=identifier)) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{%- endmacro %}
