# Bước 4 — Silver, giỏ hàng, mẫu người dùng và Gold phân tích

## Kết quả đã xác nhận từ bước 3

Bronze run: `20261020T122232Z-c66609b3`, Raw snapshot: `instacart-v1`.
Sáu tệp khớp SHA-256 sau khi đọc lại HDFS, 33 kiểm tra Bronze đều PASS.
Thời gian job Bronze trong log là 121,107 giây; đây là thời gian ingest + audit,
không phải thời gian huấn luyện mô hình hoặc benchmark nhiều máy.

| Dữ liệu | Số record đã ghi nhận |
|---|---:|
| aisles | 134 |
| departments | 21 |
| products | 49.688 |
| orders | 3.421.083 |
| order_products__prior | 32.434.489 |
| order_products__train | 1.384.617 |
| Người dùng khác nhau | 206.209 |

Phân loại orders: prior 3.214.874, train 131.209, test 75.000.
Các nhãn prior/train/test này do Instacart cung cấp; chúng chưa phải bộ chia
train/validation/test của nghiên cứu mà chúng ta sẽ thiết kế sau.

## 1. Thêm mã nguồn

Chép nội dung gói step04 vào `D:\School\BigData\instacart-basket`:

```text
scripts/prepare_silver_gold.py
scripts/test_silver_gold.py
docs/04-silver-gold.md
```

Script dùng helper có sẵn trong scripts/ingest_bronze.py của bước 3. Không cần
thay compose.yaml, rebuild image, cài thêm thư viện hoặc upload lại CSV.

## 2. Chạy job

Tại PowerShell:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose up -d --wait --wait-timeout 300
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/prepare_silver_gold.py --bronze-run 20261020T122232Z-c66609b3 --sample-bps 100 --seed 42
```

Run Bronze truyền vào phải có _READY.json và _AUDIT.json PASS. Job đọc đúng run
được chỉ định, không tự chọn một thư mục mới nhất theo tên hoặc thời gian.
Mỗi lần chạy tạo run_id mới, không ghi đè Bronze hoặc kết quả bước 4 trước đó.

Trong log sẽ có SILVER_TABLE, GOLD_TABLE, CHECK PASS và kết thúc bằng:

```text
SILVER_GOLD_PREPARE_PASS run_id=...
```

Job này xử lý toàn bộ dữ liệu đã có. Mẫu 1% chỉ áp dụng cho bộ thử mô hình xuất
ra kèm theo, không làm các bảng EDA toàn bộ dữ liệu bị thu nhỏ còn 1%.
Nếu có lỗi, dừng ở đó và gửi traceback cuối; không tự chạy lại từ bước upload.

## 3. Silver chứa gì?

| Bảng | Ý nghĩa |
|---|---|
| products | Sản phẩm nối tên aisle và department |
| orders | Giữ toàn bộ orders, gồm cả test không có nhãn sản phẩm |
| order_items | Ghép prior + train rồi nối metadata của đơn |
| baskets | Một dòng cho mỗi đơn có chi tiết sản phẩm |
| sample_users | Danh sách user được chọn và bucket lấy mẫu |
| sample_orders | Toàn bộ orders của các user đã chọn |
| sample_baskets | Toàn bộ giỏ có nhãn của các user đã chọn |

Một giỏ gồm array product_id duy nhất, sắp tăng dần, cùng user_id, order_number,
eval_set, thời điểm đặt hàng, basket_size, item_row_count và số item có reordered=1.
Thứ tự trong array là thứ tự ID để có biểu diễn ổn định; nó không phải thứ tự
thêm vào giỏ. Thứ tự thêm vào giỏ vẫn nằm trong order_items.add_to_cart_order.

Không tự xóa sản phẩm trùng để che lỗi. Mặc dù collect_set tạo tập ID, job kiểm tra
item_row_count phải bằng basket_size. Khi nguồn có dòng trùng ngoài dự kiến,
kiểm tra sẽ thất bại thay vì công bố dữ liệu đã bị âm thầm rút gọn.

Không tạo giỏ rỗng cho 75.000 đơn eval_set=test vì dữ liệu không cung cấp các
sản phẩm của những đơn đó. Các đơn test vẫn tồn tại trong bảng orders.
Không điền days_since_prior_order của đơn đầu thành 0; giữ NULL của nguồn.

Với bộ Bronze hiện tại, các mốc đối soát là:

- orders: 3.421.083 dòng.
- order_items: 33.819.106 dòng.
- baskets: 3.346.083 giỏ có nhãn.
- Tổng basket_size: 33.819.106 sản phẩm xuất hiện trong các giỏ.
- Giỏ eval_set=test: 0.

Đây là mốc suy ra từ log Bronze đã đạt, không phải số liệu mô hình.
Code lấy mốc trực tiếp từ _AUDIT.json của Bronze, không hardcode số liệu trên.

## 4. Lấy mẫu theo user

Tham số `--sample-bps 100` nghĩa là 100 basis points = khoảng 1% số người dùng.
Công thức:

```text
bucket = pmod(xxhash64(seed ép long, user_id ép long), 10000)
chọn user khi bucket < sample_bps
```

seed được đưa vào như một cột đầu vào của hash, không phải sửa seed nội bộ
của hàm xxhash64. Cùng version Spark, seed, user_id và tỷ lệ sẽ cho cùng lựa chọn,
không phụ thuộc thứ tự record hoặc số partition. Ép long làm kiểu đầu vào rõ ràng.

Tỷ lệ thực tế có thể khác 1%; số user được chọn không được ép bằng một con số có sẵn.
Khi đã chọn user, giữ toàn bộ orders và toàn bộ giỏ có nhãn của user đó.
Job so sánh hai chiều giữa lịch sử kỳ vọng và dữ liệu sample đã ghi Parquet.
Mẫu này bảo toàn lịch sử quan sát được, không khôi phục nội dung các đơn test
mà nguồn vốn không cung cấp.

Chưa chia train/validation/test nghiên cứu và chưa gọi dữ liệu này là tập train.
Bước sau sẽ thiết kế protocol rồi chia user trước khi học FP-Growth hoặc tính
Popularity. Mẫu hiện tại có thể chứa cả các user có đơn cuối train và test
theo nhãn gốc Instacart; việc chọn ca đánh giá sẽ dựa trên nhãn quan sát được.

## 5. Gold và phạm vi từng thống kê

| Bảng | Phạm vi và mẫu số |
|---|---|
| orders_by_dow | Số đơn theo mã ngày 0–6, gồm mọi orders; trung bình giỏ chỉ trên đơn có nhãn |
| orders_by_hour | Tương tự theo giờ 0–23 |
| orders_by_eval_set | Số orders theo nhãn Instacart |
| basket_size_distribution | Phân bố kích thước trên prior + train có chi tiết sản phẩm |
| product_popularity_eda | Số giỏ có sản phẩm trong prior + train; support chia cho số giỏ có nhãn |
| department_activity_eda | Lượt sản phẩm theo department; item_share chia cho tổng lượt sản phẩm có nhãn |

Trong bảng product_popularity_eda, mỗi cặp order_id/product_id đã được Bronze
xác nhận duy nhất nên số record sản phẩm bằng số đơn chứa sản phẩm đó.
Sản phẩm hoặc department không xuất hiện vẫn được giữ với count=0. reorder_rate
của nhóm không xuất hiện là NULL vì không có mẫu số, không gán thành 0.

reorder_rate là tỷ lệ lượt sản phẩm có cờ reordered=1 trên lượt sản phẩm đang xét,
không phải tỷ lệ khách hàng quay lại. Mã order_dow được giữ nguyên 0–6, chưa gắn
tên thứ trong tuần khi chưa có căn cứ từ mô tả nguồn.

Các bảng EDA dùng cả prior + train để mô tả dữ liệu hiện có. Không dùng những
bảng này làm baseline hoặc tính độ phổ biến cho mô hình đánh giá. Ở bước mô hình,
Popularity chỉ được tính trên phần dữ liệu học sau khi chia tập.

## 6. Kết quả nằm ở đâu?

HDFS:

```text
/instacart/silver/<run_id>/tables/<table>/
/instacart/gold/<run_id>/tables/<table>/
/instacart/prepared/<run_id>/_STARTED.json
/instacart/prepared/<run_id>/_REPORT.json
/instacart/prepared/<run_id>/_READY.json
```

_READY.json ở prepared là dấu công bố cho cả bộ Silver/Gold cùng run, chứa đường
dẫn đến hai tầng. Sự tồn tại của riêng một bảng hoặc _SUCCESS Parquet chưa đủ.
Nếu lỗi, các phần đã ghi được giữ để chẩn đoán; không tạo dấu READY cho bộ dữ liệu.

Trên Windows:

```text
outputs/preparation/latest.json
outputs/preparation/<run_id>/report.json
outputs/preparation/<run_id>/eda/*.csv
```

CSV chỉ chứa các bảng aggregate/danh mục để xem và dùng cho dashboard sau này;
không export toàn bộ 33 triệu dòng giao dịch về Windows. Xuất CSV theo iterator,
không dùng toPandas. Những file CSV này được đọc khi run đã PASS và READY.
latest.json chỉ cập nhật sau thành công; khi lần chạy mới lỗi, đọc report theo
run_id in ở log, không dùng latest của lần trước để kết luận lần mới thành công.

## 7. Xem kết quả và gửi lại

Sau khi lệnh chạy PASS:

```powershell
$prep = Get-Content .\outputs\preparation\latest.json -Raw | ConvertFrom-Json
$prep | Select-Object status, run_id, source_bronze_run, duration_seconds
$prep.kpis | ConvertTo-Json
$prep.sample | ConvertTo-Json -Depth 5
$prep.checks | Where-Object { $_.pass -eq $false }
$prep.top_products_eda | Select-Object product_id, product_name, order_count, labeled_order_support
```

Gửi phần tóm tắt, kpis, sample và các kiểm tra thất bại nếu có. Chưa cần gửi cả
CSV hoặc toàn bộ log Spark. Với lần chạy thành công, lệnh lọc checks không in dòng nào.

Lưu code sau khi chạy đạt:

```powershell
git add scripts/prepare_silver_gold.py scripts/test_silver_gold.py docs/04-silver-gold.md
git commit -m "Build Silver baskets user sample and descriptive Gold tables"
```

## Kiểm thử phát triển

Tùy chọn, không chạy đồng thời với job toàn bộ dữ liệu:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_silver_gold.py
```

Kiểm thử dùng dữ liệu giả lập để kiểm tra tổng đối soát, giỏ có ID được sắp xếp,
không tạo giỏ test, catalog có sản phẩm chưa xuất hiện, support/reorder_rate,
CSV có dấu phẩy và ngoặc kép, hash ổn định khi đổi thứ tự/partition/kiểu int-long,
giữ đủ ba đơn mỗi user được chọn, và phát hiện dòng sản phẩm trùng.
Đây không phải kết quả mô hình hoặc benchmark dữ liệu thật.

Trạng thái xác minh khi tạo gói: kiểm thử đã chạy đạt trên Spark 3.5.7, Java 17,
chế độ local[2], filesystem local. Có 24 phép đối soát trong luồng tạo Silver/Gold
trên bộ giả lập, cùng các kiểm tra độc lập về lấy mẫu, duplicate và CSV. Chưa chạy
job Silver/Gold trên toàn bộ dữ liệu HDFS trong môi trường tạo gói; lần chạy trên
máy Windows của bạn sẽ xác nhận phần tích hợp và các thống kê thực tế.

## Nguồn kỹ thuật

- https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xxhash64.html
- https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.functions.collect_set.html

Bước kế tiếp sau nghiệm thu là protocol đánh giá, chia user, tạo context/target,
huấn luyện FP-Growth và baseline trên đúng dữ liệu train, rồi chọn tham số bằng
validation. Chưa sử dụng test để quyết định cấu hình.
