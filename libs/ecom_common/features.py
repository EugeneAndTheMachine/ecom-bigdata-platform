"""Single definition of the purchase-intent feature vector (avoid training/serving skew).

Offline : gold.feat_session_training (dbt) produces exactly these columns.
Online  : services/recommend-api builds them from Redis `session:{id}` (written by
          pipelines/streaming/jobs/session_state.py). Keep the three in sync.
"""
INTENT_FEATURES: list[str] = [
    "n_views",          # views in the session so far
    "n_carts",          # add-to-cart events
    "cart_items",       # max(n_carts - n_removes, 0)
    "cart_value",       # net value (cart - remove_from_cart)
    "n_categories",     # distinct known category_l1, capped at 5
    "duration_s",       # seconds since session start (until first purchase offline)
    "hour_of_day",      # 0-23 of session start (UTC)
    "day_of_week",      # 1 = Monday ... 7 = Sunday (ISO, same as Trino day_of_week)
]
INTENT_LABEL = "converted"
