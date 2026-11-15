{#- Which timeline ML features are anchored on (var ml_source / env ML_SOURCE).
    Replay rows (kafka_connect) live on "now", bulk rows (bulk_csv) keep 2019-10..2020-04:
    a "last N days" window anchored on max(event_date) of both would only ever see replay.
      auto          -> bulk_csv if any bulk history exists, else kafka_connect
      bulk_csv | kafka_connect -> that source only
      all           -> no filter (only if the two timelines are not mixed) -#}
{% macro ml_source_predicate(col='source_system') -%}
  {%- set src = var('ml_source') -%}
  {%- if src == 'all' -%}
    true
  {%- elif src == 'auto' -%}
    {{ col }} = (case when exists (select 1 from {{ ref('stg_events') }} where source_system = 'bulk_csv')
                      then 'bulk_csv' else 'kafka_connect' end)
  {%- else -%}
    {{ col }} = '{{ src }}'
  {%- endif -%}
{%- endmacro %}
