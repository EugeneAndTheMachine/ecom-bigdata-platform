# ADR-0003 — Định nghĩa "order" trên REES46

- **Trạng thái:** Accepted — 2026-10-26
- **Bối cảnh:** REES46 chỉ có event (view/cart/remove_from_cart/purchase), không có order_id, không có quantity. Mỗi event `purchase` = 1 dòng sản phẩm được mua.
- **Quyết định:** `order_id = user_session || '#' || event_date`; `fact_order_items` = các event purchase; `fact_orders` = group theo order_id (revenue = Σ price, items = count). AOV = revenue / số order. Speed layer dùng cùng định nghĩa (rt_purchases), nên reconciliation so được.
- **Hệ quả:** AOV là xấp xỉ (một session có thể có nhiều lần thanh toán). Ghi rõ trong mô tả metric trên dashboard. Test dbt `assert_orders_match_purchase_events` bảo đảm Σ revenue(fact_orders) = Σ price(purchase).
