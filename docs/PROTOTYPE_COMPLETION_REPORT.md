# Nghiệm thu prototype Instacart

> Ghi chú rà file 2026-10-03: đây là nghiệm thu lịch sử. Word/PDF, Word tham chiếu và một số tài liệu cũ hiện không còn trong workspace trước lần cleanup; không diễn giải PASS phía dưới là xác nhận chúng còn tồn tại hiện tại. Xem [báo cáo cleanup](CLEANUP_REPORT.md) cho trạng thái file mới nhất.

Trạng thái: **PASS**. Run `prototype_c813268ab40a`, mode **full**, resume dữ liệu đã hoàn thành; kiểm thử raw→Gold mới chạy riêng trên mẫu thật 1%.

| Tiêu chí | Trạng thái | Bằng chứng |
|---|---|---|
| Môi trường thật, execution mode và run_id | PASS | outputs/metrics/environment.json |
| Đủ sáu CSV thật, tên/kích thước khớp | PASS | outputs/metrics/raw_profile.json |
| Schema, null, duplicate và miền giá trị | PASS | outputs/metrics/raw_profile.json; tests/test_raw_validation.py |
| Bronze Snappy, metadata và đối soát 6 bảng | PASS | data/bronze; outputs/metrics/raw_profile.json; tests/test_prototype_integration.py |
| Silver chuẩn hóa và chất lượng khóa ngoại | PASS | outputs/metrics/silver_quality.json |
| KPI và phân bố đơn/user, aisle | PASS | outputs/metrics/eda_summary.json; outputs/tables/top_20_aisles.csv |
| 12 biểu đồ thật, kèm bảng CSV | PASS | outputs/figures; outputs/tables |
| FP-Growth đủ 9 cấu hình và lý do bỏ qua | PASS | outputs/metrics/model_comparison.csv; logs/fpgrowth_pipeline.log |
| Mô hình chọn và đánh giá thật | PASS | outputs/metrics/model_summary.json |
| Gold đọc được toàn bộ bảng | PASS | data/gold; tests/test_prototype_integration.py |
| Dashboard 5 trang và Top-5 trên giỏ thật | PASS | outputs/evidence/10_dashboard_overview.png … 14_dashboard_performance.png; outputs/metrics/recommendation_evidence_basket.json |
| Spark UI thật sau action full | PASS | outputs/evidence/07_spark_ui_jobs.png; 08_spark_ui_stages.png; 09_spark_ui_environment.png; spark_session.json |
| 18 ảnh bằng chứng, nguồn và checksum | PASS | outputs/evidence/EVIDENCE_MANIFEST.md |
| Unit/integration test full thật | PASS | outputs/metrics/test_summary.json; logs/integrated_tests.log |
| Raw→Gold mẫu thật trong workspace độc lập | PASS | outputs/metrics/sample_integration.json; outputs/sample_runs/prototype_0bd00583d231 |
| Runner full/sample/auto/resume/evidence và logs | PASS | src/run_pipeline.py; run_pipeline.sh; logs/prototype_pipeline.log |
| Word 12–18 trang, TOC/PAGE, giữ nguyên bản gốc và PDF | PASS | outputs/metrics/word_validation.json; docs/Bao_cao_prototype_hoan_thien.docx; docs/Bao_cao_prototype_hoan_thien.pdf |
| README và tài liệu chạy/demo | PASS | README.md; docs/DEMO_CHECKLIST.md |

## Số liệu và thời gian thật

Lần full resume + tests + evidence: 98.378 giây; 34 test: 34.198 giây. Đây không phải thời gian huấn luyện lại toàn bộ.
Mẫu độc lập raw→Gold: 199.491 giây, 34.299 orders và 339.165 order-items; 32 test tại thời điểm snapshot.

| Bảng CSV / Bronze | Dòng | CSV bytes | Bronze bytes |
|---|---:|---:|---:|
| aisles | 134 | 2,603 | 4,717 |
| departments | 21 | 270 | 2,665 |
| products | 49,688 | 2,166,953 | 1,357,998 |
| orders | 3,421,083 | 108,968,645 | 23,722,100 |
| order_products_prior | 32,434,489 | 577,550,706 | 105,656,733 |
| order_products_train | 1,384,617 | 24,680,147 | 5,082,137 |

Silver có 33.819.106 order-items và 3.346.083 baskets. Mô hình chọn support 0,001 / confidence 0,2: 4.172 itemsets, 834 rules; fit 809,345 giây. Coverage 73.25%; HitRate@5 14.47%; median lift 2.504841.

## Phạm vi, nguồn và hạn chế

Dữ liệu full được tính ở các lần chạy có log/checkpoint; run_id hiện tại là lần kiểm tra/resume và chụp ảnh. Spark UI chứng minh action count mới trên full Silver/Gold, không giả là màn hình huấn luyện trước đó. Bronze cũ được bổ sung run_id bằng migration riêng, giữ ingestion_time gốc. Mẫu lấy từ CSV thật, giữ đầy đủ dimensions và chạy trong outputs/sample_runs; không thay thế dữ liệu full.

Chạy Spark local trên filesystem local, chưa triển khai HDFS hay cluster. Mức support 0,0005 bị bỏ qua sau thử nghiệm tài nguyên thất bại, ghi rõ trong grid. Train split đồng thời dùng chọn cấu hình và đánh giá nên kết quả là validation nội bộ, chưa là holdout độc lập. Những ô thông tin cá nhân chưa có trong Word gốc được giữ trống.

Ảnh 07–14 là ảnh trình duyệt thật; các ảnh còn lại render bảng/sơ đồ từ artifact thật và được ghi rõ trong manifest. Không có mockup hay dữ liệu giả.

## File tạo hoặc sửa

`src/evidence/*.py`, `src/run_pipeline.py`, `src/prototype_checks.py`, `src/pipeline/02_ingest_bronze.py`, `src/pipeline/03_build_silver.py`, `app/dashboard.py`, `tests/test_prototype_integration.py`, `requirements.txt`, `.streamlit/config.toml`, `.gitignore`, `README.md`; các báo cáo/CSV/PNG/manifest dưới outputs, Word/PDF và bản tham chiếu dưới docs. Danh sách artifact cụ thể nằm trong evidence manifest và prototype_summary.json.

## Chạy lại

```bash
./run_pipeline.sh --mode full --resume --evidence-mode
.venv/bin/python src/evidence/build_report.py
.venv/bin/python src/evidence/render_report.py
.venv/bin/python src/evidence/complete.py
```
