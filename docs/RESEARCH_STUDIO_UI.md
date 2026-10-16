# Research Studio UI

UI Streamlit gồm rail navy 72px, header Research Studio, tab ngang và năm trang dùng chung `studio_page`. Trên mobile, tab ngang thay rail; biểu đồ và các cột xếp dọc. CSS dùng test IDs của Streamlit và container keys, không dùng tên class sinh ngẫu nhiên. Khi nâng Streamlit cần kiểm tra lại các selector này.

`app/studio.py` quản lý shell, điều hướng native và panel cấu hình. `app/studio.css` và `.streamlit/config.toml` quản lý giao diện sáng. `app/dashboard.py` giữ các biểu đồ, bảng, bộ lọc, CSV, gợi ý và thông tin hiệu năng hiện có. Scatter hiển thị toàn bộ luật Gold; bảng và Top 20 áp dụng bộ lọc. Không huấn luyện lại khi lọc.

## Chạy cùng source mới và dữ liệu đã lưu

Mặc định app đọc `data/gold` và `outputs/metrics` tại root source. Để dùng một workspace sample thật mà không copy/ghi đè artifact, đặt đường dẫn workspace tường minh:

```bash
export INSTACART_DASHBOARD_ROOT="$PWD/outputs/sample_runs/<run_id>"
python -m streamlit run app/dashboard.py --server.port 8501
```

PowerShell (khi đã có Python/dependencies):

```powershell
$env:INSTACART_DASHBOARD_ROOT = (Resolve-Path 'outputs/sample_runs/<run_id>').Path
python -m streamlit run app/dashboard.py --server.port 8501
```

Dashboard đọc `sampling.json` của workspace để hiển thị nhãn sample, quy tắc lấy mẫu và run_id. Khi không có sampling manifest, app hiển thị workspace full; người chạy phải chọn đúng workspace và giữ nguyên manifest nguồn. Thiếu Gold/metrics hiển thị cảnh báo và hướng dẫn khôi phục trong expander. App không khởi động Spark.

Biến môi trường này chỉ chọn root dữ liệu của dashboard; không dùng dotenv. `docker_entrypoint.py dashboard-sample` dùng source UI hiện tại, chọn workspace dữ liệu theo sample manifest có trạng thái PASS và xác nhận thư mục Gold/metrics tồn tại. Với Compose, build lại image để có source UI mới rồi chạy service sample hiện có:

```bash
docker compose --profile sample up --build dashboard-sample
```

## Kiểm tra

Với cùng biến workspace đã chọn:

```bash
python -m unittest discover -s tests -p test_dashboard.py -v
python -m playwright install chromium
python tests/capture_research_studio.py --url http://127.0.0.1:8501
```

Script browser dùng Gold thật, kiểm tra năm trang, chọn giỏ, fallback, bộ lọc rỗng, tải CSV và kích thước 1440/1024/390. Có thể thêm `--missing-url` trỏ tới server thứ hai với workspace trống để kiểm tra thiếu dữ liệu. Ảnh và CSV kiểm tra ghi vào thư mục riêng `outputs/evidence/research_studio_20261016`, không ghi đè bằng chứng lịch sử. Ảnh là viewport của vùng cuộn Streamlit; có ảnh bổ sung phần bảng luật.

## Kiểm tra mới ngày 16/10/2026

- Fetch `origin/dev`: cập nhật từ `5fe1d3f` lên `71d2713`. Working tree ban đầu sạch. Nhánh `feature/research-studio-ui` được tạo trực tiếp từ `origin/dev` ở `71d2713`; không commit/push.
- Dữ liệu kiểm tra: workspace sample `prototype_253438e8b758`, quy tắc `order_id % 10000 < 100`. Gold full ở root không có; không chạy pipeline hoặc training.
- Docker kiểm thử riêng dùng Python 3.12. Các thư viện UI chính khớp requirements: Streamlit 1.64.0, Plotly 7.1.0, Pandas 3.0.6, PyArrow 25.0.1, Playwright 1.63.0. Đây không phải kiểm chứng cài sạch toàn bộ requirements hoặc Python 3.14.
- Bộ `test_dashboard.py`: 8/8 PASS trên Gold sample thật, gồm kiểm tra nghiệp vụ gợi ý, fallback, năm trang, thiếu dữ liệu và đồng bộ rail–tab.
- Kiểm tra lệnh khởi động `docker_entrypoint.py` bằng mock `os.execv` với manifest sample thật: cả full/sample dùng source `/app/app/dashboard.py`; sample trỏ đúng workspace dữ liệu. Không chạy pipeline trong phép kiểm tra này.
- Kết quả trình duyệt và ảnh: xem `browser_checks.json` trong thư mục bằng chứng mới; chỉ coi các kiểm tra đã ghi trong file này là hoàn tất.
- `git diff --check`: PASS. Không sửa pipeline, recommender, model config, raw, checkpoint hoặc metrics lịch sử.

Full Gold, full training và toàn bộ integration suite Spark chưa được kiểm tra trong task UI này.
