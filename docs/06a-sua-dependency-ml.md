# Sửa lỗi ModuleNotFoundError: numpy

## Nguyên nhân

Image apache/spark:3.5.7-java17-python3 đang dùng trên máy bạn không có NumPy.
Spark SQL của các bước trước vẫn chạy, nhưng import pyspark.ml yêu cầu NumPy.
Lệnh bị dừng ngay khi import, trước khi tạo run_id/model hoặc huấn luyện.
Kiểm thử trước đó dùng môi trường có sẵn NumPy; cấu hình image thiếu dependency.

## Thay đổi

Gói sửa gồm:

```text
compose.yaml
docker/spark/Dockerfile
docker/spark/requirements-ml.txt
scripts/test_ml_runtime.py
docs/06a-sua-dependency-ml.md
```

Spark Master và Worker cùng dùng image build cục bộ instacart-spark:3.5.7-ml-v1,
kế thừa image Apache cũ và thêm numpy==1.26.4, setuptools==75.8.2 (cung cấp
distutils cho các import tương thích của PySpark 3.5 khi cần). Không cài thêm một phiên bản
PySpark khác qua pip. Dockerfile kiểm tra import MLlib ngay khi build.
Build context chỉ là docker/spark nên không đưa data/raw vào build context.

Hai dịch vụ Hadoop và hai named volumes giữ nguyên cấu hình.
Các script/dữ liệu/mô hình đã lưu trên HDFS tiếp tục dùng lại.

## 1. Chép gói sửa

Giải nén gói sửa rồi chép nội dung vào D:\School\BigData\instacart-basket,
chấp nhận thay compose.yaml bằng bản trong gói.

## 2. Build image và tạo lại hai container Spark

Chạy lần lượt trong PowerShell:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose config --quiet
docker compose build spark-master
docker compose up -d --no-deps --force-recreate --wait --wait-timeout 300 spark-master spark-worker
```

Chỉ cần build spark-master vì cả hai service cùng dùng một image/tag và cùng
build context. Worker sử dụng image vừa build khi được tạo lại.
Không dùng docker compose down -v; không cần nạp dữ liệu hoặc chia tập lại.
Các lệnh này khởi động lại Master/Worker nên chạy khi không có job Spark khác.
Trong tình huống lỗi import hiện tại, lệnh huấn luyện đã dừng trước khi mở job.

Từ lúc áp dụng bản sửa, image Spark được build từ Dockerfile; khi cần chuẩn bị
lại môi trường dùng docker compose build spark-master. Nếu muốn pull riêng
image Hadoop, dùng docker compose pull namenode datanode.

## 3. Kiểm tra dependency cả driver và executor

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_ml_runtime.py
```

Mong đợi cuối log có ML_RUNTIME_PASS, NumPy 1.26.4, executor host spark-worker.
Script import NumPy/FP-Growth ở driver và worker rồi fit một mô hình giả lập
bốn giỏ, kiểm tra frequent itemsets và confidence đã biết.
Nó không ghi đè dataset, model hoặc test result; không đọc tập test nghiên cứu.

## 4. Chạy lại đúng bước huấn luyện

Sau ML_RUNTIME_PASS:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/train_validate.py --split-run 20261020T124751Z-e4b31e98
```

Đợi MODEL_VALIDATION_PASS rồi xem selected/validation_metrics và tiếp tục lệnh
evaluate_test.py như hướng dẫn bước 6. Nếu vẫn lỗi thì dừng ở đó, gửi log cuối.
Không dùng latest.json của run cũ để tiếp tục sau một lệnh huấn luyện thất bại.

Nếu build lỗi mạng khi tải wheel, gửi phần lỗi pip/build. Nếu Dockerfile báo
không import được PySpark, gửi dòng ML_IMPORT_PASS hoặc traceback ở bước build.

## Lưu mã

```powershell
git add compose.yaml docker/spark/Dockerfile docker/spark/requirements-ml.txt scripts/test_ml_runtime.py docs/06a-sua-dependency-ml.md
git commit -m "Add pinned NumPy to shared Spark ML image"
```

## Nguồn

Xác minh khi tạo gói: kiểm tra YAML và xác nhận cấu hình Hadoop/named volumes
không đổi; kiểm thử import và fit giả lập đã PASS với Spark 3.5.7 trong môi trường
Python 3.12 mới, chỉ bổ sung NumPy 1.26.4 và setuptools 75.8.2 vào các dependency
của Spark. Không có Docker Engine ở môi trường tạo gói để build image thực tế;
bước build và ML_RUNTIME_PASS trên máy bạn sẽ xác nhận phần container.

https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html
