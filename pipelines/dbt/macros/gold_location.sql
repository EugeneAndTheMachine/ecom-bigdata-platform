{#-
  Fixed storage location for Gold tables that Spark (ML jobs) reads BY PATH.
  Usage in a model:  {{ config(properties=gold_location('feat_user_offline')) }}
  Must stay in sync with ecom_common.tables.gold_path().
-#}
{% macro gold_location(name) -%}
  {{ return({"location": "'" ~ var('lakehouse_root') ~ "/gold/" ~ name ~ "'"}) }}
{%- endmacro %}
