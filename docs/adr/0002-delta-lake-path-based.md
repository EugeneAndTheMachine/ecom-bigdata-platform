# ADR-0002 — Delta Lake, Spark ghi theo path, Trino đọc qua Hive Metastore

- **Trạng thái:** Accepted — 2026-10-26
- **Bối cảnh:** Cần table format ACID (MERGE, time travel, replaceWhere) cho medallion. Trino là query engine cho dbt/BI. Spark 3.5 dùng Hive client 2.3.x; Hive Metastore 4.x đã bỏ các Thrift API cũ (`get_table`…), nên Spark ↔ HMS 4 không tương thích trực tiếp.
- **Quyết định:**
  - Table format **Delta Lake 3.3.2** (đi với Spark 3.5.5).
  - Spark đọc/ghi **theo path** (`s3a://lakehouse/<layer>/<table>`), không dùng catalog. Mọi path khai báo một chỗ: `libs/ecom_common/tables.py`.
  - HMS (apache/hive 4.0.0, backend Postgres) **chỉ** phục vụ Trino Delta connector. Bảng do Spark tạo được đăng ký bằng `CALL delta.system.register_table(...)` (`pipelines/batch/register_tables.py`, idempotent).
  - Gold do dbt-trino tạo; các bảng Gold mà Spark ML cần đọc được đặt **location cố định** qua macro `gold_location()`.
- **Hệ quả:** (+) Không phụ thuộc phiên bản Hive client của Spark; Spark job portable (chạy được với `LAKEHOUSE_ROOT=file:///…`). (−) Hai "chủ" ghi khác nhau (Spark: bronze/silver/ml; Trino: gold) → quy ước *mỗi bảng chỉ một writer*. (−) `CREATE OR REPLACE` của Trino trên bảng location cố định cần kiểm tra khi nâng Trino.
- **Phương án đã cân nhắc:** Iceberg + REST catalog (tốt cho multi-engine nhưng thêm service, đội quen Delta hơn); Spark ↔ HMS 3.1 (phải pin HMS cũ, Trino 476 vẫn chạy được nhưng giữ 2 đường catalog).
