select
    product_id,
    category_id,
    category_code,
    category_l1,
    category_l2,
    category_l3,
    brand,
    price,
    attr_hash,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'products_scd2') }}
