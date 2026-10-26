# Notebooks

Chạy trong container `jupyter` (`make up-bi`, mở http://localhost:8888, token = `JUPYTER_TOKEN`).
Notebook chỉ dùng để **khám phá / prototype**. Code production nằm ở `pipelines/` và `libs/` —
khi một đoạn logic đã ổn định, chuyển nó vào module + unit test, notebook chỉ import lại.

| Notebook | Mục đích |
|---|---|
| `01_eda_rees46.ipynb` | Phân phối event, null rate, độ dài session, giá → chốt DQ rules & features |

Quy ước đặt tên: `NN_<chủ_đề>.ipynb`; clear output trước khi commit.
