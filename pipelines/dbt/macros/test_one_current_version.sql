{#- SCD2 integrity: every product has exactly one is_current version. -#}
{% test one_current_version_per_product(model) %}
select product_id, count_if(is_current) as n_current
from {{ model }}
group by product_id
having count_if(is_current) <> 1
{% endtest %}
