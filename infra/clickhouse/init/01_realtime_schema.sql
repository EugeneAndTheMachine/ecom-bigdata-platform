-- =============================================================================
--  Speed-layer serving tables (written by Spark foreachBatch, read by Grafana)
--  ReplacingMergeTree + deterministic ORDER BY key  => re-delivered micro-batches
--  collapse into one row (effectively exactly-once at the sink). Query with FINAL
--  (or argMax) for exact results before background merges happen.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS realtime;

-- Q1: funnel per 1-minute tumbling window x top-level category
CREATE TABLE IF NOT EXISTS realtime.rt_funnel_1m
(
    window_start   DateTime('UTC'),
    window_end     DateTime('UTC'),
    category_l1    LowCardinality(String),
    views          UInt64,
    carts          UInt64,
    removes        UInt64,
    purchases      UInt64,
    revenue        Float64,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, category_l1)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- Q2: trending products, 10-minute sliding window (slide 1 minute)
CREATE TABLE IF NOT EXISTS realtime.rt_trending_10m
(
    window_start   DateTime('UTC'),
    window_end     DateTime('UTC'),
    category_l1    LowCardinality(String),
    product_id     UInt64,
    views          UInt64,
    carts          UInt64,
    purchases      UInt64,
    score          Float64,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(window_end)
ORDER BY (window_end, category_l1, product_id)
TTL toDateTime(window_end) + INTERVAL 7 DAY;

-- Closed sessions (session window, gap 30 min)
CREATE TABLE IF NOT EXISTS realtime.rt_sessions
(
    user_session   String,
    user_id        UInt64,
    session_start  DateTime64(3, 'UTC'),
    session_end    DateTime64(3, 'UTC'),
    duration_s     Float64,
    n_events       UInt32,
    n_views        UInt32,
    n_carts        UInt32,
    n_purchases    UInt32,
    revenue        Float64,
    updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(session_start)
ORDER BY (toDate(session_start), user_session)
TTL toDateTime(session_start) + INTERVAL 30 DAY;

-- Q3: cart-abandonment alerts (also published to Kafka alerts.cart_abandon)
CREATE TABLE IF NOT EXISTS realtime.rt_cart_abandon
(
    user_session    String,
    user_id         UInt64,
    cart_items      UInt32,
    cart_value      Float64,
    last_event_time DateTime64(3, 'UTC'),
    alerted_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(alerted_at)
PARTITION BY toYYYYMMDD(last_event_time)
ORDER BY (toDate(last_event_time), user_session)
TTL toDateTime(last_event_time) + INTERVAL 30 DAY;

-- Q4: bot alerts (also published to Kafka alerts.bot)
CREATE TABLE IF NOT EXISTS realtime.rt_bot_alerts
(
    window_start   DateTime('UTC'),
    user_id        UInt64,
    event_count    UInt32,
    zscore         Float64,
    detected_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, user_id)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- Purchases enriched by stream-static join with silver dim_product (replaces CDC rt_orders)
CREATE TABLE IF NOT EXISTS realtime.rt_purchases
(
    event_id       String,
    event_time     DateTime64(3, 'UTC'),
    user_id        UInt64,
    user_session   String,
    product_id     UInt64,
    category_l1    LowCardinality(String),
    category_code  String,
    brand          LowCardinality(String),
    price          Float64,
    list_price     Nullable(Float64),
    ingested_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (toDate(event_time), event_id)
TTL toDateTime(event_time) + INTERVAL 30 DAY;

-- ---------------------------------------------------------------------------
-- Convenience views for Grafana
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS realtime.v_funnel_last_5m AS
SELECT
    category_l1,
    sum(views)     AS views,
    sum(carts)     AS carts,
    sum(purchases) AS purchases,
    round(if(sum(views) = 0, 0, sum(carts) / sum(views)), 4)         AS view_to_cart,
    round(if(sum(carts) = 0, 0, sum(purchases) / sum(carts)), 4)     AS cart_to_purchase,
    round(if(sum(views) = 0, 0, sum(purchases) / sum(views)), 4)     AS view_to_purchase,
    sum(revenue)   AS revenue
FROM realtime.rt_funnel_1m FINAL
WHERE window_start >= now() - INTERVAL 5 MINUTE
GROUP BY category_l1;

CREATE VIEW IF NOT EXISTS realtime.v_revenue_1m AS
SELECT
    toStartOfMinute(event_time) AS minute,
    category_l1,
    count()                     AS orders,
    sum(price)                  AS revenue,
    avg(price)                  AS avg_price
FROM realtime.rt_purchases FINAL
GROUP BY minute, category_l1;

-- Live consistency check: purchases counted by the funnel job vs. enriched purchase stream
CREATE VIEW IF NOT EXISTS realtime.v_consistency_1m AS
SELECT f.minute, f.funnel_purchases, p.enriched_purchases,
       f.funnel_purchases - p.enriched_purchases AS diff
FROM
  (SELECT window_start AS minute, sum(purchases) AS funnel_purchases
   FROM realtime.rt_funnel_1m FINAL GROUP BY minute) AS f
LEFT JOIN
  (SELECT toStartOfMinute(event_time) AS minute, count() AS enriched_purchases
   FROM realtime.rt_purchases FINAL GROUP BY minute) AS p
USING (minute);
