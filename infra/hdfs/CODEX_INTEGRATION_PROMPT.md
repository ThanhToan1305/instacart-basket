# Tích hợp HDFS vào pipeline Instacart hiện tại

Làm việc trực tiếp trong repository đang mở. Giữ pipeline local hiện có và kết quả prototype. Không tạo dự án mới, không tự nâng cấp dependencies và không push Git.

## Khảo sát trước khi sửa

1. Đọc AGENTS.md nếu có, README, setup guide, cấu hình, runner, stage Bronze/Silver/Gold, evaluation và dashboard.
2. Xác định các đường dẫn hiện tại, cách đọc/ghi Parquet, checkpoint, fingerprint, lock và cấu trúc artifact.
3. Kiểm tra `git status`; giữ thay đổi chưa commit của người dùng.
4. Kiểm tra HDFS bằng `bash infra/hdfs/hdfs.sh status` và JSON smoke gần nhất. Không format, reset hoặc xóa dữ liệu HDFS.

## Mục tiêu triển khai

- Thêm lựa chọn storage_backend local/hdfs theo cơ chế cấu hình hiện tại.
- Dữ liệu phân tích trên HDFS dùng URI tuyệt đối `hdfs://127.0.0.1:9000/...`; không tự thay tất cả đường dẫn local bằng HDFS.
- Đọc raw thật dưới `/instacart/raw`; giữ kiểm tra input, schema, chất lượng và đối soát raw/Bronze.
- Ghi Bronze/Silver/Gold của lần chạy mới vào namespace riêng theo run_id. Không ghi đè prototype cũ hoặc dùng lại Gold cũ rồi gọi là lần chạy HDFS.
- Dùng Spark/Hadoop FileSystem API cho tồn tại, list, metadata và rename trên HDFS. Không truyền URI hdfs cho pathlib, os.path hoặc open().
- Tách đường dẫn log, metrics, evidence, config và local checkpoint với data URI; bảo toàn lock và quy tắc resume. Fingerprint phải gắn backend, URI, cấu hình và nguồn phù hợp.
- HDFS không thay thế Spark master. Giữ Spark local hiện tại; không khai báo cluster nhiều máy.
- Nếu dashboard chỉ hỗ trợ local, thêm bước export Gold cần thiết vào thư mục mới, có manifest nguồn HDFS/run_id. Giữ lỗi thiếu đầu ra rõ ràng; không tự fit khi mở dashboard.

## Kiểm tra và đầu ra

1. Chạy test local hiện có bị ảnh hưởng. Thêm test có ý nghĩa cho URI, backend, thiếu input và resume khác backend.
2. HDFS integration test dùng dataset sample thật và quan hệ khóa hợp lệ. Không thay dữ liệu thật bằng dữ liệu giả để tính KPI. Tiny synthetic chỉ được dùng cho unit/smoke và phải ghi rõ.
3. So sánh schema, số dòng, KPI và dữ liệu đầu ra của cùng sample giữa local và HDFS. Tách sai khác float nếu có theo tolerance được định nghĩa.
4. Không chạy full khi chưa đo dung lượng, RAM và sample chưa PASS. Nếu thiếu input hoặc HDFS chưa hoạt động, báo blocker chính xác, không bịa kết quả.
5. Cập nhật README: start/stop HDFS, các lệnh runner đúng của source, URI, export dashboard, checkpoint và phạm vi giả phân tán.
6. Kết thúc bằng danh sách file sửa, lệnh đã chạy, test PASS/FAIL/SKIP và phần còn cần xác minh. Chỉ nêu HDFS hoàn thành khi pipeline mẫu đã chạy thực sự qua backend này.
