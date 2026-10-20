# Bước 3 — Nạp Raw và kiểm tra Bronze

## Điều kiện đầu vào

Bước 2 đã đạt trên máy Windows: bốn container healthy, một DataNode hoạt động,
Spark Standalone thực thi task trên spark-worker, smoke test đọc/ghi HDFS PASS.
Sáu CSV nằm trực tiếp trong data/raw, được mount read-only thành /input ở NameNode.

## Phạm vi

Chuyển toàn bộ sáu CSV lên HDFS, tạo bản sao Parquet có kiểu dữ liệu và audit.
Chưa xây giỏ, chia mẫu, huấn luyện FP-Growth hoặc đánh giá gợi ý ở bước này.
Chúng ta kiểm tra toàn bộ dữ liệu đầu vào trước; mô hình ban đầu vẫn sẽ dùng mẫu.

Bronze giữ nguyên các dòng được parser CSV đọc, không tự xóa trùng, điền thiếu,
lọc sản phẩm hoặc sửa giá trị. Chỉ chuyển CSV sang kiểu dữ liệu khai báo và Parquet.
Parser dừng khi gặp giá trị sai kiểu/record malformed theo chế độ FAILFAST;
header được so theo đúng thứ tự, không ép schema bỏ qua tên header.
CSV dùng dấu phẩy, UTF-8, quote và escape là dấu ngoặc kép, không hỗ trợ record
nhiều dòng trong gói này. Nếu phát hiện trường văn bản nhiều dòng cần điều chỉnh
có chủ đích rồi chạy lại; không tự bỏ bản ghi lỗi.

Các dòng trắng có thể bị parser CSV bỏ qua; số dòng trong audit là số record
được parser đọc, không phải số ký tự xuống dòng trong tệp. Bản Raw giữ nguyên bytes.

## 1. Thêm gói bổ sung

Chép nội dung gói step03 vào `D:\School\BigData\instacart-basket`:

```text
scripts/upload_raw.py
scripts/ingest_bronze.py
scripts/test_bronze_audit.py
tests/test_raw_upload.py
docs/03-raw-bronze.md
```

Không thay compose.yaml và không cần pull lại image. Thư mục scripts đã được
bind mount vào Spark Master nên container nhìn thấy tệp mới ngay.
Host chạy upload_raw chỉ cần thư viện chuẩn Python và Docker CLI.
PySpark chạy bên trong image Spark; không cần pip install PySpark trên Windows.

## 2. Xác nhận stack còn chạy

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose up -d --wait --wait-timeout 300
python scripts/check_data.py
```

## 3. Upload Raw một lần

```powershell
python scripts/upload_raw.py --snapshot instacart-v1
```

Script tính SHA-256 của từng tệp trên Windows, chép từng tệp từ /input vào HDFS,
đọc lại toàn bộ bytes từ HDFS để kiểm tra hash. Vì có đọc lại, thao tác này lâu
hơn việc chỉ chép file. Lệnh cần kết thúc bằng:

```text
RAW_UPLOAD_PASS snapshot=instacart-v1 files=6 path=/instacart/raw/instacart-v1
```

Manifest được ghi tại `outputs/raw/instacart-v1.json`. HDFS có:

```text
/instacart/raw/instacart-v1/
  aisles.csv
  departments.csv
  products.csv
  orders.csv
  order_products__prior.csv
  order_products__train.csv
  _READY.json
```

_READY.json chỉ được ghi sau khi cả sáu tệp đã đối chiếu hash thành công.
Tên snapshot đã tồn tại thì script dừng, không ghi đè. Không chạy lại upload
nếu đã có RAW_UPLOAD_PASS. Khi upload dở hoặc lỗi checksum, giữ nguyên snapshot
chưa hoàn thành để chẩn đoán; không tự xóa volume hoặc format NameNode.

Kiểm tra:

```powershell
docker compose exec -T namenode /opt/hadoop/bin/hdfs dfs -ls /instacart/raw/instacart-v1
```

## 4. Tạo Bronze và chạy audit

Sau khi upload PASS:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/ingest_bronze.py --snapshot instacart-v1
```

Mỗi lần gọi sinh run_id mới. Script chỉ đọc Raw đã READY, ghi Parquet có nén Snappy,
đọc Parquet để audit và lưu kết quả theo run. Nó sử dụng 64 shuffle partitions
để các phép distinct/join của bảng lớn có thể chia nhỏ trên worker hiện tại.
Không collect bảng giao dịch về driver; chỉ collect các thống kê nhỏ.

Lệnh in `INGEST_TABLE ...` và từng `CHECK PASS/FAIL ...`. Thời gian chạy phụ thuộc
CPU, SSD, Docker và dữ liệu; không kết luận bị treo chỉ vì một job lâu.
Trong khi chạy có thể xem http://localhost:4040 hoặc docker stats ở terminal khác.

Audit kiểm tra:

- Sáu bảng không rỗng; kiểu dữ liệu, null bắt buộc, chuỗi tên rỗng, ID/số thứ tự dương.
- Khóa đơn của bốn bảng danh mục/orders và khóa kép order_id/product_id của hai bảng chi tiết.
- orders: eval_set hợp lệ; ngày trong tuần 0–6; giờ 0–23; days_since_prior_order
  thiếu đúng ở đơn đầu, còn lại trong 0–30 và không phải NaN.
- products tham chiếu được aisle_id và department_id.
- Không trùng cặp user_id/order_number.
- Chi tiết đơn tham chiếu được sản phẩm và đơn; prior/train khớp eval_set.
- reordered là 0 hoặc 1; vị trí thêm vào giỏ không trùng và liên tục 1..k.
- Mỗi đơn prior/train có ít nhất một dòng chi tiết. Đơn test không có nhãn sản phẩm
  trong hai CSV chi tiết nên không yêu cầu điều này đối với test.

Audit này chưa kiểm tra toàn bộ tính nhất quán theo lịch sử người dùng, như
ý nghĩa reordered qua các đơn hoặc khoảng cách thời gian tích lũy. Không gọi
PASS là bảo đảm tuyệt đối về toàn vẹn hoặc chất lượng mô hình.

Đầu ra:

```text
/instacart/bronze/<run_id>/
  _STARTED.json
  tables/aisles/
  tables/departments/
  tables/products/
  tables/orders/
  tables/order_products__prior/
  tables/order_products__train/
  _AUDIT.json
  _READY.json
```

_SUCCESS của một bảng Parquet chỉ xác nhận bảng đó đã ghi xong. Chỉ _READY.json
ở cấp run mới xác nhận toàn bộ kiểm tra của gói này đã PASS.
Nếu lỗi, run không được đánh dấu READY, báo cáo có FAILED và có thể có _FAILED.json.
Các bảng đã ghi được giữ để chẩn đoán, không bị xóa tự động.
Sau khi sửa nguyên nhân có thể chạy lại ingest trên cùng Raw snapshot; run_id mới
sẽ được tạo. Không cần upload lại Raw nếu hash/READY của Raw đã đạt.

## 5. Đọc báo cáo và gửi kết quả

Mong đợi cuối log: `BRONZE_INGEST_PASS run_id=...`.

```powershell
$audit = Get-Content .\outputs\ingest\latest.json -Raw | ConvertFrom-Json
$audit | Select-Object status, run_id, source_snapshot, duration_seconds, distinct_users
$audit.tables | ConvertTo-Json -Depth 5
$audit.orders_by_eval_set | ConvertTo-Json
$audit.checks | Where-Object { $_.pass -eq $false }
```

Lệnh cuối không in dòng nào nếu các kiểm tra đều PASS. Gửi phần tóm tắt, số dòng
của sáu bảng và orders_by_eval_set. latest.json chỉ cập nhật khi thành công; nếu
lần chạy vừa lỗi, xem báo cáo theo run_id in ở đầu/cuối log, không dùng latest cũ
để kết luận lần chạy mới thành công.

Nếu job lỗi, gửi đoạn traceback cuối cùng và:

```powershell
docker compose ps -a
docker compose logs --tail 100 spark-worker
docker stats --no-stream
```

## 6. Lưu mã nguồn bằng Git

Sau khi chạy thành công:

```powershell
git add scripts/upload_raw.py scripts/ingest_bronze.py scripts/test_bronze_audit.py tests/test_raw_upload.py docs/03-raw-bronze.md
git commit -m "Add verified HDFS raw upload and audited Bronze ingestion"
```

CSV và outputs vẫn thuộc .gitignore có sẵn. Giữ outputs cho báo cáo nhưng không
commit toàn bộ dữ liệu vào Git.

## Kiểm thử cho người phát triển

Upload unit checks không cần Docker/PySpark:

```powershell
python -m unittest discover -s tests -p test_raw_upload.py
```

Audit trên tập giả lập (tùy chọn, không cần chạy lại khi đang xử lý full data):

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_bronze_audit.py
```

Kiểm thử audit bao gồm bộ hợp lệ, dữ liệu cố tình sai khóa/FK/miền giá trị/vị trí,
CSV có dấu phẩy/ngoặc kép, record sai kiểu số phải bị FAILFAST và roundtrip
helper JSON qua Hadoop FileSystem.
Kết quả kiểm thử giả lập không được dùng làm số liệu thực nghiệm.

Trạng thái kiểm chứng khi tạo gói: năm unit checks của upload đã đạt. Bộ kiểm thử
Spark đã chạy đạt trên Spark 3.5.7, Java 17, chế độ local[2], Hadoop FileSystem local:
33 kiểm tra đều đạt với dữ liệu hợp lệ, các lỗi cố tình tạo được phát hiện,
CSV có quote được đọc đúng, record sai kiểu số bị từ chối, JSON roundtrip đúng.
Chưa chạy upload thật vào HDFS hoặc ingest toàn bộ Instacart trong môi trường
tạo gói; hai việc này sẽ được xác nhận trên stack Docker của bạn.

## Nguồn kỹ thuật

- CSV: https://spark.apache.org/docs/3.5.7/sql-data-sources-csv.html
- HDFS Shell: https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/FileSystemShell.html

Bước tiếp theo sau nghiệm thu: Silver chuẩn hóa, tạo giỏ và chọn mẫu theo người dùng
để giữ lịch sử khi thiết kế đánh giá. Kết quả mô hình sẽ được chạy lại từ đầu.
