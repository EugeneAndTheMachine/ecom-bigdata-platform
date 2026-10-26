select distinct
    category_id,
    coalesce(category_code, 'unknown') as category_code,
    category_l1,
    category_l2,
    category_l3
from {{ ref('stg_products') }}
where is_current
