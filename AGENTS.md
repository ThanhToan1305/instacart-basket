# Hướng dẫn Agent trong Instacart Prototype

## Mục tiêu và bản đồ source

Phân tích hành vi Instacart thật bằng Spark SQL/DataFrame và MLlib FP-Growth, gợi ý mua kèm Top-5 qua Streamlit. CSV → Bronze → Silver → Analytics/FP-Growth → Gold → Dashboard. EDA ghi Gold KPI trước training; không giả định Gold luôn được tạo sau ML.

- `run_pipeline.sh`, `src/run_pipeline.py`: runner Linux (fcntl/flock), full/sample/auto, fingerprint, verified resume, tests, optional evidence.
- `src/pipeline/raw_validation.py`: sáu tên CSV/schema tường minh, khóa và miền giá trị.
- `01_validate_raw.py`, `02_ingest_bronze.py`: validation, Bronze Snappy thêm ingestion_time/source_file/run_id.
- `03_build_silver.py`: wrapper canonical của `03_build_thưsilver.py`; `silver_helpers.py`: DQ/FK.
- `04_eda.py`: Spark aggregations, SQL percentile, export bounded aggregates ≤500 rows; Gold KPI.
- `05_train_fpgrowth.py`, `fpgrowth_worker.py`, `fpgrowth_helpers.py`: prior baskets integer unique ≥2, một fit/support, ba confidence, worker timeout/cap.
- `06_evaluate_rules.py`: prior cuối làm context, train làm truth/selection; Gold itemsets/rules/lookup.
- `src/common/`: SparkSession từ YAML và logger; `config/settings.yaml`, `config/model_config.yaml`: cấu hình thật.
- `app/dashboard.py`: năm trang; `data_loader.py`: cache Gold/metrics, không khởi động Spark; `recommender.py`: khớp đủ antecedent, loại context, rank confidence/lift/support/product_id; fallback count thật không bịa metric.
- `src/evidence/`: sample isolated, supplement, browser screenshots, Word/PDF và nghiệm thu; `src/prototype_checks.py`: verifier; `src/write_chapter5.py`: Markdown từ metrics.
- `tests/`: unittest raw, Silver/EDA, FP-Growth, dashboard, runner safety và integration. `notebooks/` hiện chưa có notebook.

## Lệnh đã xác minh từ source

Chạy ở root; setup chi tiết trong `docs/SETUP_GUIDE.md`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip check
bash -n run_pipeline.sh
python src/run_pipeline.py --help
python -m unittest discover -s tests -p test_pipeline_runner.py -v
python -m unittest discover -s tests -p test_silver_eda.py -v
bash run_pipeline.sh --mode sample --sample-fraction 0.01
bash run_pipeline.sh --mode full --resume
python -m streamlit run app/dashboard.py --server.port 8501
python -m unittest discover -s tests -v
```

Đây là danh sách theo nhu cầu, không phải chuỗi phải chạy toàn bộ mỗi task. Full tests cần dataset, Gold/Silver, checkpoint và metrics thật. Evidence thêm `--evidence-mode`, cần Chromium/cổng 4040. Không chạy pipeline nặng khi task docs chỉ cần đọc source/test nhẹ. Project không dùng `.env` hoặc dotenv; không thêm dependency này.

## Quy tắc bắt buộc

1. Đọc README, setup, cấu trúc và code liên quan trước sửa; xem Git status nếu Git hợp lệ. Hướng dẫn người dùng hiện tại được ưu tiên.
2. Không thay Instacart bằng dữ liệu giả; không sửa raw; thiếu dataset/dependency thì báo đúng thứ thiếu, làm phần độc lập trước. Không fake PASS hoặc kết quả mô hình.
3. Không commit dataset, Parquet, venv, log, `.env`, secrets hay config tài khoản. Không commit/push nếu người dùng chưa yêu cầu.
4. Dùng đường dẫn tương đối hoặc root từ pathlib; không hard-code đường dẫn máy. Báo cáo runtime cũ có absolute path: chỉ đề xuất sửa khi ngoài phạm vi, không rewrite bằng chứng cũ.
5. Không đổi schema Bronze/Silver/Gold nếu chưa rà checker/tests/dashboard và yêu cầu task. Không sửa support/confidence/score/coverage để số đẹp hơn.
6. Giữ prior-only training, full antecedent matching, distinct recommendations và context exclusion. Train đang dùng chọn cấu hình: phải gọi validation nội bộ.
7. Không xóa/rename/move dataset, outputs/evidence, checkpoint hay báo cáo có thật để ép rerun. Runner/stages từ chối overwrite; dùng verified resume hoặc workspace mới theo yêu cầu.
8. Không collect/toPandas bảng lớn. Bảng nhỏ cần giới hạn kiểm tra rõ; một row aggregate/first được dùng trong source.
9. Chạy kiểm tra phù hợp sau sửa code; docs dùng kiểm tra link/lệnh/ignore, bash syntax, runner safety. Không tự chạy full training để kiểm tra docs.
10. Báo file sửa/tạo, test mới chạy, kết quả lịch sử và phần chưa xác minh riêng biệt. Không in token/password khi scan.

## Quy ước và hoàn thành

Giữ Python conventions/UTF-8, pathlib root, logger và JSON status PASS/FAIL/RESUMED đang dùng; script pipeline có số thứ tự, entry point `main`. Giữ wrapper Silver và implementation legacy; tránh đổi tên vì tài liệu/checkpoint phụ thuộc. Task hoàn thành khi đúng phạm vi, checker/test thích hợp pass, không phá artifact và docs/lệnh phản ánh code thật. Không đánh đồng RESUMED với huấn luyện lại.

Không sửa tự ý: `data/`, metrics/screenshot lịch sử, Word tham chiếu, `src/doc/` task gốc, configs model/schema, checkpoint ẩn, secrets/tool caches. Không sửa nhãn full/sample hoặc timestamp để giả cùng một lần chạy.

## Các vấn đề kỹ thuật đã biết

- Người dùng xác nhận dự án chỉ chạy local, chưa push lần nào. `.git` hiển thị rỗng trong sandbox, xóa báo resource busy và ngoài sandbox không thấy thư mục; có thể là mount môi trường, không có metadata/history Git đã xác minh. Không tự khởi tạo/commit repository thật.
- Requirements là snapshot rộng 125 packages, ghim tương lai theo môi trường hiện có; mới kiểm tra installed versions/pip check, chưa thử install sạch/mọi nền tảng. Runner Linux-only do fcntl và shell `.venv/bin/python`; Windows dùng WSL2.
- Một số task/báo cáo/Word tham chiếu và src/doc hiện không còn trên đĩa trước lần cleanup. Không coi các kết quả lịch sử là bằng chứng bộ tài liệu hiện tại đầy đủ. Metrics runtime chứa đường dẫn máy; giữ local, không sửa provenance âm thầm.
- Support 0,0005 chưa hoàn tất do tài nguyên/gián đoạn; đánh giá chưa là holdout độc lập; Spark local chưa HDFS/cluster.
- Fingerprint raw dựa tên/size/mtime, chưa content hash toàn CSV; di chuyển/clone artifact khác máy chưa được bảo đảm resume.
- `src/evidence/complete.py` có kỳ vọng full snapshot cụ thể và cần sample manifest thật; không dùng cho mọi dataset/config mới. Renderer yêu cầu LibreOffice user-local.
- Clone source không có Gold/metrics/checkpoint; nhiều test cần build hoặc bundle dữ liệu riêng trước. Không chạy smoke spark.range như bằng chứng Instacart.

Chi tiết ở `docs/GITHUB_READINESS_REPORT.md`; `.gitignore` chỉ ảnh hưởng file chưa track. Không tự untrack file cũ khi chưa được yêu cầu.
