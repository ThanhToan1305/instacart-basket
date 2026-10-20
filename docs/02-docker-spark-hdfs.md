# Bước 2 — Docker, Spark Standalone và HDFS

## Mục tiêu

Chạy được Spark Master + một Worker và HDFS NameNode + một DataNode.
Spark ghi Parquet vào HDFS, đọc lại và đối soát dữ liệu. Đây là kiểm tra hạ tầng,
chưa phải pipeline Instacart, huấn luyện FP-Growth hoặc kết quả thực nghiệm.

## Cấu hình dành cho máy hiện tại

Máy Windows: RAM khoảng 16 GB, 12 luồng CPU. Docker đang thấy 8.165.052.416 bytes
(khoảng 7,60 GiB). Chưa cần tăng RAM Docker cho bước này.

| Dịch vụ | Giới hạn RAM container | Thiết lập chính |
|---|---:|---|
| namenode | 1 GiB | Java heap tối đa 384 MiB |
| datanode | 768 MiB | Java heap tối đa 384 MiB |
| spark-master | 2 GiB | Master daemon 256 MiB và driver 1 GiB khi chạy job |
| spark-worker | 3 GiB | Worker daemon 256 MiB; cung cấp 2 lõi, 2 GiB; executor heap 1536 MiB |

Tổng giới hạn khoảng 6,75 GiB; lượng dùng thực tế thay đổi theo job. Chạy tuần tự
một job. Không mở thêm stack Docker của project cũ cùng lúc nếu thiếu RAM.
Các giới hạn container bao gồm cả heap, Python và phần bộ nhớ ngoài heap.

Image được cố định: `apache/hadoop:3.4.2` và
`apache/spark:3.5.7-java17-python3`. Python/Java/Spark dùng bên trong container;
không cần cài thêm Java, Spark, Hadoop hoặc PySpark trên Windows cho bước này.

Tất cả dịch vụ chạy trên cùng một máy. Worker là tiến trình riêng trong container,
nhưng đây chưa phải cụm nhiều máy. HDFS chỉ có một bản sao (`dfs.replication=1`).
NameNode và DataNode dùng named volumes để giữ metadata và block qua lần dừng/mở.
Hadoop dùng tài khoản root trong môi trường học tập; các cổng UI chỉ bind loopback.

## 1. Thêm bộ cấu hình vào project

Giải nén gói bổ sung ở thư mục tạm, rồi chép nội dung vào:
`D:\School\BigData\instacart-basket`.

Các tệp được thêm:

```text
compose.yaml
.gitattributes
docker/hadoop/core-site.xml
docker/hadoop/hdfs-site.xml
docker/hadoop/start-namenode.sh
docker/spark/spark-defaults.conf
scripts/smoke_test.py
docs/02-docker-spark-hdfs.md
```

Gói không chứa CSV. Dữ liệu hiện có vẫn nằm trong `data/raw`.
File shell phải giữ xuống dòng LF; mở bằng VS Code nếu cần và chọn LF ở thanh trạng thái.

## 2. Kiểm tra Compose rồi tải image

Chạy lần lượt trong PowerShell, tại thư mục gốc:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose config --quiet
docker compose pull
```

`config --quiet` thành công sẽ không in gì và có mã thoát 0. Nếu một lệnh lỗi,
dừng tại đó và gửi lỗi. Lần đầu tải image có thể lâu tùy mạng và dung lượng image.

## 3. Khởi động bốn dịch vụ

```powershell
docker compose up -d --wait --wait-timeout 300
docker compose ps
docker compose exec -T namenode /opt/hadoop/bin/hdfs dfsadmin -report
```

Mong đợi bốn dịch vụ `healthy`, và HDFS có `Live datanodes (1)`.
NameNode chỉ format tự động khi volume hoàn toàn trống. Khi có dữ liệu cũ, script
khởi động giữ nguyên metadata; nếu thư mục bất thường, script dừng để kiểm tra.

## 4. Kiểm tra Spark thực thi trên Worker và đọc/ghi HDFS

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/smoke_test.py
```

Chương trình kiểm tra có Worker ALIVE, chạy Python task trên `spark-worker`,
tạo 1.000 dòng giả lập với id 0–999, ghi vào đường dẫn HDFS mới theo run_id,
đọc lại, kiểm tra count=1000, sum(id)=499500 và mười nhóm mỗi nhóm 100 dòng.
Các con số này chỉ là giá trị kỳ vọng của bài kiểm tra kỹ thuật.

Mong đợi cuối log xuất hiện `SMOKE_TEST_PASS`. Mỗi lần chạy tạo thư mục HDFS riêng:
`/instacart/checks/<run_id>`. Báo cáo kiểm tra được ghi ra Windows tại:
`outputs/checks/latest.json` và `outputs/checks/<run_id>.json`.

Xem báo cáo:

```powershell
Get-Content .\outputs\checks\latest.json
docker compose exec -T namenode /opt/hadoop/bin/hdfs dfs -ls /instacart/checks
```

Nếu job lỗi thì không coi báo cáo cũ là kết quả lần chạy mới. Đối chiếu run_id và
chỉ chấp nhận khi lệnh vừa chạy kết thúc thành công, có dòng `SMOKE_TEST_PASS`.

## 5. Mở UI

| Trang | URL | Kiểm tra |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | Một DataNode hoạt động |
| Spark Master | http://localhost:8080 | Một Worker ALIVE với 2 cores, 2 GiB |
| Spark Worker | http://localhost:8081 | Trạng thái worker và executor log |
| Spark application | http://localhost:4040 | Chỉ có khi job còn chạy |

Các hostname `namenode`, `datanode`, `spark-master`, `spark-worker` là DNS nội bộ
Docker. Các lệnh HDFS/Spark được chạy bên trong container bằng `docker compose exec`.
Không cần sửa file hosts của Windows. Tải file bằng HDFS UI có thể điều hướng tới
hostname DataNode nội bộ; dùng HDFS CLI để thao tác dữ liệu ở bước này.

## 6. Dừng và mở lại

Kết thúc buổi làm việc:

```powershell
docker compose stop
```

Mở lại vào buổi sau:

```powershell
docker compose up -d --wait --wait-timeout 300
```

Lệnh `docker compose down` cũng giữ named volumes. Không thêm `-v` vì tùy chọn đó
xóa volume HDFS của project. Không format NameNode lại để xử lý lỗi khởi động.

## 7. Khi có lỗi

Gửi kết quả các lệnh sau cùng thông báo của lệnh đang lỗi:

```powershell
docker compose ps -a
docker compose logs --tail 100 namenode datanode spark-master spark-worker
docker stats --no-stream
```

- Lỗi pull: gửi tên image và nguyên thông báo lỗi mạng/tag.
- Cổng bị chiếm: xác định project đang dùng 9870, 8080, 8081 hoặc 4040 trước khi đổi cổng.
- `Permission denied` khi ghi báo cáo: kiểm tra Docker Desktop được truy cập thư mục project.
- `Incompatible clusterIDs`: kiểm tra cặp volume; không tự xóa/format để chữa lỗi.
- Spark báo chưa có tài nguyên: kiểm tra Worker ALIVE và không còn job khác chạy.
- Container `Exited (137)` hoặc OOMKilled: gửi logs và docker stats để điều chỉnh bộ nhớ.

## Nghiệm thu và bước tiếp

Bước này hoàn thành khi bốn dịch vụ healthy, HDFS có một DataNode, và smoke test PASS.
Sau đó mới nạp sáu CSV lên HDFS, kiểm tra số dòng/khóa/giá trị và xây Bronze–Silver–Gold.
Chưa đưa dữ liệu giả lập của smoke test vào báo cáo kết quả mô hình.

## Nguồn và trạng thái xác minh

- Spark Standalone: https://spark.apache.org/docs/3.5.7/spark-standalone.html
- Hadoop HDFS: https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html
- Hadoop image: https://hub.docker.com/r/apache/hadoop/tags
- Spark image: https://hub.docker.com/r/apache/spark/tags

Bộ cấu hình đã được kiểm tra cú pháp YAML/XML/shell/Python và tính nhất quán các
đường dẫn mount trong môi trường tạo gói. Môi trường đó không có Docker Engine,
nên chưa chạy được integration test; kết quả Docker/Spark/HDFS thực tế sẽ được
xác nhận bằng những lệnh ở trên trên máy bạn.
