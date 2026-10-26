# ADR-0001 — Thu hẹp scope: chỉ dùng REES46 (bỏ F2 CDC, F3 API ngoài)

- **Trạng thái:** Accepted — 2026-10-26
- **Bối cảnh:** Thiết kế gốc có 3 nhóm nguồn: F1 clickstream (REES46), F2 CDC Postgres (Olist) qua Debezium, F3 API ngoài (Open-Meteo, Frankfurter, Google Trends) + Kaggle Olist/Amazon review. Nhóm quyết định chỉ giữ REES46.
- **Quyết định:**
  - Bỏ hẳn Postgres OLTP + Debezium + topic `cdc.*`; bỏ toàn bộ DAG ingest API ngoài; bỏ sentiment/NLP.
  - Doanh thu (Q5) tính từ event `purchase` của clickstream. *Order* được định nghĩa = (user_session × ngày) có ≥ 1 purchase (REES46 không có order_id) — xem ADR-0003.
  - Q5 rút gọn: doanh thu/AOV theo ngày/danh mục/brand (không vùng miền, thời tiết, tỷ giá).
  - Q8 thay bằng: *ảnh hưởng của thay đổi giá (SCD2) lên conversion* — tận dụng việc giá REES46 thay đổi theo thời gian.
  - Reconciliation chỉ còn 2 chiều: Silver (batch) ↔ ClickHouse (speed).
- **Hệ quả:** Kiến trúc gọn hơn (−5 service), tập trung chất lượng stream + batch trên 1 nguồn lớn (~285M event/7 tháng). Mất khả năng phân tích yếu tố ngoài; có thể thêm lại sau dưới dạng flow mới mà không đổi lõi (Bronze/Silver giữ schema riêng theo nguồn).
