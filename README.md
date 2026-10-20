# Phân tích hành vi mua sắm và gợi ý sản phẩm mua kèm

Nhóm 10 — Apache Spark 3.5.7, Hadoop/HDFS 3.4.2, FP-Growth và Streamlit.
Môi trường thực nghiệm: Docker Desktop Linux trên một máy Windows; một Spark
Master và một Worker. Đây là mô phỏng cụm trên một máy, không phải nhiều máy vật lý.

## Các phần đã triển khai

- Raw: sáu CSV Instacart, kiểm tra header và SHA-256 trước/sau upload HDFS.
- Bronze: schema rõ ràng, Parquet và kiểm tra chất lượng dữ liệu.
- Silver/Gold: giỏ hàng, sản phẩm, mẫu theo user giữ nguyên lịch sử và bảng EDA.
- Thực nghiệm: chia user train/validation/test, che một phần giỏ để đánh giá hoàn thiện giỏ.
- Mô hình: Spark ML FP-Growth, Popularity và Hybrid, chọn tham số bằng validation.
- Test: lưu dự đoán và kết quả, đóng băng mô hình theo split.
- Dashboard: năm trang Streamlit, đọc bản export cùng mô hình đã đánh giá.

EDA dùng toàn bộ dữ liệu theo phạm vi ghi trên trang. Mô hình dùng mẫu user và
chỉ học từ giỏ prior của user train. Popularity được tính từ tập học. Ranking,
ghép gợi ý và chấm điểm dùng Python trên driver với tập luật có giới hạn kích thước.

## Máy đã có dữ liệu và kết quả

Mở PowerShell tại thư mục project. Để mở dashboard từ export đã có:

```powershell
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --no-deps --wait --wait-timeout 180 dashboard
```

Mở http://localhost:8501. Dashboard không cần Spark/HDFS đang chạy khi đã có
`outputs/dashboard/latest.json` và thư mục export tương ứng.

Để mở lại cụm xử lý:

```powershell
docker compose build spark-master spark-worker
docker compose up -d --wait --wait-timeout 300
docker compose ps
```

Image Spark có NumPy trong `docker/spark/requirements-ml.txt`; build image này
trước khi chạy các script Spark ML.

## Tái lập từ đầu

Đặt sáu CSV gốc vào `data/raw`, bật Docker Desktop rồi làm theo tài liệu tuần tự:

| Bước | Tài liệu |
|---|---|
| Môi trường và smoke test | docs/02-docker-spark-hdfs.md |
| Raw và Bronze | docs/03-raw-bronze.md |
| Silver, Gold và mẫu user | docs/04-silver-gold.md |
| Protocol và split | docs/05-protocol-va-chia-tap.md |
| Mô hình và test | docs/06-huan-luyen-va-danh-gia.md |
| Dependency ML của image hiện tại | docs/06a-sua-dependency-ml.md |
| Export và dashboard | docs/07-dashboard-streamlit.md |
| Nghiệm thu và đóng gói kết quả | docs/08-dong-goi.md |

Các run ID trong tài liệu ghi lại lần chạy thực tế đã quan sát. Khi tái lập mới,
thay bằng run ID vừa được script in ra, không tái sử dụng ID của lần chạy cũ.
Tài liệu bước 0–1 mô tả thời điểm khởi tạo, không phải trạng thái triển khai hiện tại.

## Kết quả đã ghi nhận

Model `20261020T131023Z-cae680de`: minSupport 0,002; minConfidence 0,40;
91 luật; 1.668 frequent itemsets; 22.117 giỏ học. Validation có 271 ca, test có 306 ca.

| Phương pháp | HitRate@5 validation | HitRate@5 test |
|---|---:|---:|
| Popularity | 27,6753% | 22,8758% |
| FP-Growth rule-only | 2,5830% | 0,6536% |
| Hybrid | 28,4133% | 23,2026% |

Hybrid chênh Popularity khoảng 0,327 điểm phần trăm trên test, tương đương
chênh lệch ròng một ca có hit. Một sample/seed/mask và 306 ca test chưa đủ để
khẳng định ưu thế tổng quát. Rule-only dùng cấu hình được chọn cho Hybrid,
chưa được tối ưu riêng. Dashboard đọc kết quả từ artifact, không dùng bảng này
làm nguồn dữ liệu. Không điều chỉnh tham số dựa trên test đã mở.

## Bảo toàn và bàn giao

`docker compose stop` dừng dịch vụ và giữ dữ liệu; `docker compose down` giữ named
volumes. `docker compose down -v` xóa named volumes chứa HDFS.

```powershell
python scripts/package_project.py
```

Lệnh này tạo ZIP mã nguồn/kết quả trong `outputs/delivery`, không chạy lại mô hình.
ZIP chứa serving export cho dashboard, không chứa CSV gốc, image Docker, volume
HDFS, Parquet các tầng hoặc mô hình Spark đã lưu trên HDFS. Muốn tái lập đầy đủ
cần sáu CSV gốc và chạy lại pipeline; muốn phục hồi nguyên trạng HDFS cần phương
án sao lưu volumes riêng. Không đưa dữ liệu thô và outputs vào Git.
