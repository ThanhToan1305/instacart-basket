# Prompt tích hợp pipeline HDFS vào source đang mở

Tiếp tục trên repository Instacart hiện tại. Môi trường chính là Docker Desktop Windows, không yêu cầu người dùng mở Ubuntu hoặc cài Hadoop trên host. Giữ image ứng dụng, dashboard hiện tại và kết quả prototype cũ.

## Khảo sát

1. Đọc AGENTS.md nếu có, compose.yaml, compose.hdfs.yaml, Dockerfile, docker_entrypoint.py, run_pipeline.sh, cấu hình, runner, các stage, recommender và dashboard.
2. Kiểm tra git status; không ghi đè thay đổi người dùng. Xác định cách sample, resume, checkpoint, fingerprint và lock đang hoạt động.
3. Kiểm tra log HDFS, health và JSON smoke. Không coi smoke là pipeline full. Không format, xóa volume hoặc dùng down -v.

## Triển khai

- Thêm lựa chọn storage local/hdfs vào cơ chế cấu hình hiện có; mặc định local giữ nguyên hành vi.
- Spark dùng URI tuyệt đối hdfs://namenode:9000 trong cùng mạng Compose. Cấu hình dfs.client.use.datanode.hostname=true. Không dùng localhost để gọi NameNode từ container khác.
- Đọc đúng sáu CSV thật trên HDFS. Hỗ trợ kiểm tra tồn tại, schema, số dòng và chất lượng qua Hadoop FileSystem/Spark; không dùng pathlib/open/os.path cho URI HDFS.
- Ghi Bronze/Silver/Gold HDFS của lần chạy mới theo run_id. Tách đường dẫn dữ liệu với log/metrics/evidence local và checkpoint hiện có.
- Fingerprint/checkpoint gắn backend, URI, input và cấu hình; tránh resume nhầm kết quả local sang hdfs. Giữ cơ chế fail fast và lock.
- Chỉ thêm service job HDFS sau khi hiểu entrypoint; không giả định biến HDFS_URI tự được source sử dụng. Giữ jobs/sample profiles và cổng dashboard hiện tại.
- Dashboard chưa hỗ trợ HDFS trực tiếp thì xuất đúng tập Gold cần đọc ra local, kèm manifest nguồn HDFS/run_id. Không tự fit lại khi mở giao diện.

## Xác minh

- Chạy test local bị ảnh hưởng, test định tuyến URI, thiếu input, resume khác backend và export manifest.
- Chạy pipeline sample thật qua local và HDFS với cùng quy tắc lấy mẫu. So sánh schema, số dòng, chất lượng, KPI và đầu ra quan trọng; xác định tolerance float nếu cần.
- Không bịa dữ liệu hoặc kết quả; dữ liệu kỹ thuật chỉ dành cho smoke/unit test, không thay dataset.
- Không chạy full trước khi sample đạt và đã kiểm tra RAM, dung lượng Docker disk. Không tự nâng cấp PySpark/Python/Java hoặc chuyển Spark sang cluster.
- Cập nhật README và demo checklist với các lệnh Compose đã kiểm chứng. Chưa push Git.

Kết thúc: báo file thay đổi, lệnh đã chạy, kết quả PASS/FAIL/SKIP, artifact và blocker cụ thể. Chỉ báo tích hợp HDFS hoàn thành sau khi sample thực sự chạy qua HDFS và được đối soát.
