# ADR-0004 — Airflow 3 (LocalExecutor) + dbt-trino cho Gold

- **Trạng thái:** Accepted — 2026-10-26
- **Quyết định:**
  - Airflow **3.3.0**, LocalExecutor, các thành phần tách container (api-server, scheduler, dag-processor, triggerer). Spark job submit bằng `SparkSubmitOperator` (client mode, driver chạy trong container scheduler → `spark.driver.host=airflow-scheduler`).
  - Lịch cron dùng `CronTriggerTimetable` mặc định của Airflow 3 ⇒ `{{ ds }}` = ngày chạy, nên DAG daily xử lý **D-1** bằng `{{ macros.ds_add(ds, -1) }}`.
  - dbt cài trong venv riêng `/opt/dbt-venv` (tránh xung đột dependency với Airflow), chạy bằng BashOperator `dbt build` (model + test trong 1 lệnh; test fail ⇒ task fail).
  - Asset (data-aware scheduling) đánh dấu output Silver/Gold/ML để DAG downstream có thể trigger theo dữ liệu thay vì giờ.
- **Phương án đã cân nhắc:** Dagster (asset-first tốt nhưng đội đã quen Airflow), Astronomer Cosmos (render dbt model thành task — sẽ thêm khi số model lớn).
