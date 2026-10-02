# Hướng dẫn cài đặt và chạy Instacart Prototype

Các lệnh Bash dưới đây chạy từ thư mục gốc repository, trừ bước clone. Dữ liệu và môi trường ảo không đi kèm source. Không cần `.env`: ứng dụng đọc `config/settings.yaml` và `config/model_config.yaml`; không dùng dotenv.

## 1. Yêu cầu hệ thống

Môi trường đã chạy thành công: Linux/WSL2, Python 3.14.4, Java 17, PySpark 4.2.0. `requirements.txt` là file cài đặt chính thức; `requirements-lock.txt` là snapshot chính xác của môi trường đã kiểm tra, không phải lock theo hash hay bảo đảm mọi nền tảng đều cài được. Danh sách hiện có cả thư viện Jupyter và dependencies gián tiếp; chưa rút gọn trong task tài liệu này.

Spark 4.2 hỗ trợ Python 3.10+ và Java 17/21/25 theo [tài liệu Apache Spark](https://spark.apache.org/docs/4.2.0/). Tuy nhiên toàn bộ phiên bản ghim trong requirements mới được xác minh ở Python 3.14.4, không suy ra chúng đều hỗ trợ Python cũ hơn. Không tự nâng cấp hàng loạt khi pip báo lỗi.

Dataset raw hiện khoảng 681 MiB; Parquet, staging, checkpoint và backup cần thêm dung lượng. Dành tối thiểu 10 GiB trống cho dữ liệu/đầu ra, cộng chỗ cho venv và công cụ ảnh/Word. Máy full đã chạy với khoảng 5,65 GiB RAM tổng, nhưng mức support 0,0005 chưa hoàn tất. Khi máy hạn chế tài nguyên, dùng mẫu 1%; không coi full là luôn chạy được với từng ấy RAM.

## 2. Windows, WSL2 Ubuntu và VS Code

Trong PowerShell quyền Administrator, cài WSL và Ubuntu, khởi động lại nếu được yêu cầu:

```powershell
wsl --install -d Ubuntu
wsl --list --verbose
```

Các bước theo [Microsoft: cài WSL](https://learn.microsoft.com/en-us/windows/wsl/install). Cài VS Code và extension WSL; mở terminal Ubuntu để clone và chạy project. Không dùng venv Windows cho Linux. Sau khi clone, có thể mở project trong VS Code từ Ubuntu:

```bash
code .
```

Lệnh này cần VS Code và extension WSL đã cài; xem [Microsoft: môi trường WSL](https://learn.microsoft.com/en-us/windows/wsl/setup/environment).

## 3. Kiểm tra Java và Python

```bash
java -version
python3 --version
git --version
```

Nếu thiếu Java hoặc công cụ cơ bản trên Ubuntu:

```bash
sudo apt update
sudo apt install openjdk-17-jdk git python3-venv
```

Lệnh apt không bảo đảm Python mặc định của mọi Ubuntu là 3.14.4. Chuẩn bị Python 3.14 phù hợp với requirements trước bước tạo venv; việc cài mới trên máy khác chưa được kiểm chứng trong lần rà tài liệu này. Spark cài qua pip, không yêu cầu một bản Spark tải riêng hoặc `SPARK_HOME`.

## 4. Clone và tạo môi trường

Chưa xác minh được URL GitHub của dự án. Thay `<REPOSITORY_URL>` bằng URL thật nhóm cung cấp; đây là placeholder, không phải remote đã được cấu hình.

```bash
git clone <REPOSITORY_URL> instacart-prototype
cd instacart-prototype
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip check
```

Không copy `.venv` từ thành viên khác. Kiểm tra đúng interpreter và các phiên bản:

```bash
python -c "import sys, pyspark, streamlit; print(sys.version); print(pyspark.__version__); print(streamlit.__version__)"
```

Cấu hình Spark chung ở `config/settings.yaml`: local[*], driver 2g, shuffle 32. FP-Growth ở `config/model_config.yaml`: driver 3g, 128 partitions, task_cpus 4, Top-5. Máy cá nhân thay đổi cấu hình phải ghi nhận; runner sẽ từ chối resume nếu fingerprint raw/config thay đổi. Đừng sửa cấu hình rồi dùng kết quả cũ như thể đã huấn luyện bằng cấu hình mới.

## 5. Đặt dataset và kiểm tra đầu vào

Lấy sáu CSV Instacart thật qua kênh dataset riêng của nhóm, đặt đúng tên trong `data/raw/`:

```text
aisles.csv
departments.csv
products.csv
orders.csv
order_products__prior.csv
order_products__train.csv
```

Hai tên order_products có **hai dấu gạch dưới**. Không thêm dữ liệu giả khi thiếu file. Kiểm tra tên, kích thước và header nhẹ, không khởi động Spark:

```bash
python -c "import sys; sys.path.insert(0, 'src'); from pipeline.raw_validation import inspect_files; print(inspect_files('data/raw'))"
```

Để Spark kiểm tra schema tường minh, null khóa, duplicate, miền giá trị và số dòng:

```bash
python src/pipeline/01_validate_raw.py
```

Lệnh này đọc toàn bộ CSV và ghi lại `outputs/metrics/raw_profile.json`/CSV. Không chạy nó chỉ để xem nhanh ở workspace đã có báo cáo Bronze: kết quả validation mới thay báo cáo trước đó. Ingestion tự validation nên không bắt buộc chạy validation riêng trước runner.

## 6. Chạy pipeline

Lựa chọn phù hợp cho clone mới/máy hạn chế RAM: mẫu giao dịch thật 1%, giữ đủ dimensions và tách workspace dưới `outputs/sample_runs/<run_id>`:

```bash
bash run_pipeline.sh --mode sample --sample-fraction 0.01
```

Mẫu chọn theo `order_id % 10000 < 100`, không đảm bảo đủ lịch sử của từng người dùng hay đúng 1% ở mọi bảng. Kết quả sample không thay thế Gold full tại gốc. Để mở dashboard mẫu, lấy run_id/path workspace từ `outputs/metrics/sample_integration.json`, đổi `<run_id>` tương ứng rồi chạy:

```bash
python -m streamlit run outputs/sample_runs/<run_id>/app/dashboard.py --server.port 8502
```

Chạy toàn bộ full hoặc xác minh/resume đầu ra đã có:

```bash
bash run_pipeline.sh --mode full --resume
```

Chế độ tự chọn: giữ full khi có model_summary; workspace mới có RAM khả dụng dưới 6 GiB sẽ chọn sample, còn lại full. Kiểm tra mode trong báo cáo, đừng suy ra từ tên lệnh:

```bash
bash run_pipeline.sh --mode auto --resume
```

Runner thực tế: raw → Bronze → Silver → EDA/KPI → FP-Growth → evaluation → supplement → kiểm tra Gold → product catalog → tests; optional evidence sau tests. Runner có lock, fingerprint raw (tên/kích thước/mtime) và nội dung YAML, dừng khi stage lỗi. `--no-resume` chỉ cho workspace sạch, không xóa đầu ra có sẵn. Các pipeline stage từ chối ghi đè bảng đã tồn tại.

Nếu muốn chạy từng bước, chỉ dùng trên workspace phù hợp, theo thứ tự sau; **không chạy lại trên dữ liệu đã publish**:

```bash
python src/pipeline/02_ingest_bronze.py
python src/pipeline/03_build_silver.py
python src/pipeline/04_eda.py
python src/pipeline/05_train_fpgrowth.py
python src/pipeline/06_evaluate_rules.py
python app/prepare_dashboard_data.py
```

`03_build_silver.py` gọi implementation cũ `03_build_thưsilver.py`, không phải hai stage khác nhau. EDA tạo Gold KPI trước bước ML; evaluation tạo Gold model. Supplement được runner gọi với run_id, không thêm một stage tưởng tượng vào luồng.

## 7. Dashboard và bằng chứng

Dashboard chỉ đọc Gold và metrics; không tự chạy Spark. Với full đã có đầu ra:

```bash
python -m streamlit run app/dashboard.py --server.address 127.0.0.1 --server.port 8501
```

Mở <http://127.0.0.1:8501>. Dừng bằng Ctrl+C trong terminal chạy server. Nếu thiếu Gold/metrics, app báo thiếu dữ liệu; chạy pipeline ở đúng workspace, không copy Gold sample vào full.

Chụp bằng chứng thật cần Chromium của Playwright và cổng Spark UI 4040 khả dụng:

```bash
python -m playwright install chromium
bash run_pipeline.sh --mode full --resume --evidence-mode
```

Evidence mode tạo action Spark mới đọc Silver/Gold rồi chụp Jobs/Stages/Environment; không giả là UI của lần fit trước. Với sample, capture dùng app của workspace mẫu và cổng 8502. Capture có thể khởi động dashboard nền; dừng đúng PID nếu cần, không kill hàng loạt process.

Bản Word/PDF được tạo ở lần nghiệm thu trước nhưng hiện không còn trong docs; Word tham chiếu cũng đang thiếu. Phải phục hồi bản tham chiếu gốc trước khi chạy lại các lệnh dưới đây. Tạo lại chỉ khi đủ artifact full và môi trường công cụ; renderer hiện yêu cầu bản LibreOffice user-local do helper chuẩn bị, không tự tìm LibreOffice hệ thống:

```bash
python src/evidence/local_office.py
python src/evidence/build_report.py
python src/evidence/render_report.py
python src/evidence/complete.py
```

Helper tải/giải nén gói apt vào `outputs/tools`, cần index/network và dung lượng riêng; Word/PDF không phải điều kiện để mở dashboard. `complete.py` hiện nghiệm thu snapshot full cụ thể, không phải validator tổng quát cho mọi sample.

## 8. Kiểm thử

Test runner nhẹ không cần dataset:

```bash
python -m unittest discover -s tests -p test_pipeline_runner.py -v
```

Test EDA đọc artifact thật, cần đầu ra đã hoàn tất:

```bash
python -m unittest discover -s tests -p test_silver_eda.py -v
```

Toàn bộ test cần CSV/Silver/Gold và checkpoint phù hợp; có test Spark đọc training/evaluation. Clone chỉ có source không thể chạy đủ suite thành công ngay:

```bash
python -m unittest discover -s tests -v
```

`src/test_spark.py` là smoke test lịch sử dùng spark.range 1.000.000 số, không phải dataset Instacart và không được dùng thay kết quả dữ liệu thật. Task chuẩn bị GitHub không cần chạy smoke này.

## 9. Lỗi thường gặp

| Triệu chứng | Xử lý |
|---|---|
| Java không được nhận diện | Chạy `java -version`; cài OpenJDK 17 trong cùng Ubuntu/WSL chạy Python. Nếu dùng JAVA_HOME, trỏ đúng JDK của máy, không copy đường dẫn máy khác. |
| ModuleNotFoundError | Kích hoạt `.venv`, kiểm tra interpreter; chạy pip install từ requirements bằng chính Python đang dùng. |
| Dataset không tìm thấy | Kiểm tra sáu tên file, double underscore, nonempty/header; chạy từ root, không sửa raw để né validation. |
| Java heap/OOM, worker timeout | Đọc log worker; giảm workload bằng mode sample ở workspace riêng. Không tăng memory vượt RAM khả dụng; không chỉnh kết quả/nhãn PASS bằng tay. |
| Streamlit port đã dùng | Dừng server do mình mở hoặc chọn cổng khác; không dừng ứng dụng người khác. |
| Raw/config changed since checkpoint | Không sửa fingerprint bằng tay; phục hồi input tương ứng hoặc rebuild trong workspace khác. |
| Existing output/refusing overwrite | Dùng runner resume để kiểm tra artifact; không xóa bảng hay checkpoint để ép chạy lại. |
| Runner bị khóa | Xác minh có runner đang chạy; lock dùng flock, không suy ra cứ thấy file lock là phải xóa. |
| pip không tìm thấy phiên bản ghim | Kiểm tra Python/platform/index/network; requirements được xác minh ở môi trường hiện tại, chưa thử cài lại sạch. Báo nhóm, không âm thầm nâng cấp toàn bộ. |

Lệnh dashboard cổng khác:

```bash
python -m streamlit run app/dashboard.py --server.port 8503
```

## 10. Sau khi restart máy

Mở Ubuntu, vào thư mục clone bằng VS Code/terminal, kích hoạt venv. Nếu dữ liệu/Gold vẫn còn, chỉ mở dashboard:

```bash
source .venv/bin/activate
python -m streamlit run app/dashboard.py --server.port 8501
```

Nếu cần xác minh pipeline, dùng resume đã nêu, không chạy no-resume trên đầu ra cũ. Xem [README](../README.md), [workflow nhóm](TEAM_WORKFLOW.md) và [báo cáo dọn file hiện tại](CLEANUP_REPORT.md) trước khi chia sẻ source.
