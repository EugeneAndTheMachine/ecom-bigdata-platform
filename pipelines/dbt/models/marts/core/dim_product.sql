-- SCD Type 2 product dimension (history built by Spark MERGE in silver.products_scd2)
{{ config(properties=gold_location('dim_product')) }}

select
    to_hex(sha256(to_utf8(cast(product_id as varchar) || '|' || cast(valid_from as varchar)))) as product_key,
    product_id,
    category_id,
    category_code,
    category_l1,
    category_l2,
    category_l3,
    brand,
    price                                                  as list_price,
    valid_from,
    valid_to,
    is_current
from {{ ref('stg_products') }}
