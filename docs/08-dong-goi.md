# Bước 8 — Nghiệm thu và đóng gói kết quả

Mục tiêu: tạo một gói mã nguồn và kết quả có cùng nguồn gốc để chuẩn bị báo cáo,
slide và demo. Lệnh đóng gói chỉ đọc tệp local; không cần Docker đang chạy,
không fit lại, không dự đoán lại trên test và không sửa kết quả.

## 1. Nghiệm thu demo

Kiểm tra trực tiếp năm trang trước buổi trình bày:

| Trang | Điều cần xác nhận |
|---|---|
| Tổng quan | 206.209 user; 3.421.083 đơn; 3.346.083 giỏ có nhãn; 49.688 sản phẩm |
| Hành vi mua sắm | Biểu đồ giờ/ngày, kích thước giỏ và ngành hàng hiển thị |
| Luật kết hợp | Có 91 luật, bộ lọc chỉ thay nội dung hiển thị |
| Thử giỏ hàng | Chọn sản phẩm, xem đủ ba phương pháp; gợi ý loại sản phẩm đã chọn |
| Đánh giá & tái lập | Test có 306 ca; HR@5 Popularity 22,8758%, Hybrid 23,2026%; có giới hạn thí nghiệm |

Ảnh Tổng quan chỉ xác nhận trang đó. Việc các trang khác chạy được cần kiểm tra
trực tiếp. Có thể dùng ví dụ từ một luật ở trang thử giỏ hàng để minh họa, ghi rõ
đó là ví dụ chủ động chọn từ luật, không phải ca test ngẫu nhiên.

## 2. Gom đúng artifact

Chép gói bước 8 vào thư mục project, giữ cấu trúc thư mục, rồi chạy:

```powershell
Set-Location D:\School\BigData\instacart-basket
python scripts/package_project.py
```

Khi đạt sẽ in `PACKAGE_PASS` và đường dẫn `archive=...zip`. File được tạo trong
`outputs/delivery`. Mỗi lần có tên riêng, không đè lên lần trước.

Script kiểm tra checksum bản dashboard, xác nhận dữ liệu export thực, lần theo
model → split → preparation → Bronze → raw manifest, đối chiếu report local với
report đã export, xác nhận protocol/serving và các kiểm tra PASS trong report.
Nó không thay thế một lần kiểm tra dữ liệu hoặc tính đúng của metric độc lập.

Gói chứa:

- Mã nguồn, cấu hình Docker và hướng dẫn hiện tại.
- Raw manifest có hash; report Bronze, preparation, split, model và test đúng run.
- Protocol, grid, dự đoán validation/test và CSV metric đã lưu.
- Catalog, bảng Gold và serving export của dashboard.
- HANDOFF_AUDIT.json: run ID, tóm tắt metric và SHA-256 của từng file.

Gói không chứa CSV thô, Docker images, volumes HDFS, toàn bộ Parquet hoặc bản
Spark model trong HDFS. Nó dùng để bàn giao kết quả và chạy dashboard, không phải
bản sao lưu đầy đủ cụm. Giữ project và volumes hiện tại trên máy.

Nếu báo `PACKAGE_FAILED`, giữ nguyên kết quả và gửi nguyên dòng lỗi. Ví dụ thiếu
file hoặc report không cùng run cần xử lý nguồn gốc; không sửa tay metric hoặc
đổi status PASS để vượt qua bước kiểm tra.

## 3. Chạy dashboard từ gói bàn giao

Giải nén sang thư mục riêng, vào thư mục `instacart-basket` bên trong, bật Docker
Desktop. Tạo `data/raw` và `outputs` nếu hệ điều hành chưa có các thư mục này.

```powershell
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --no-deps --wait --wait-timeout 180 dashboard
```

Mở http://localhost:8501. Gói đã có export nên không cần chạy lại Spark, HDFS hoặc
huấn luyện. Nếu một project cùng tên đang chạy trên máy, dừng dashboard cũ trước
và dùng tên project Compose khác khi mở bản sao để tránh dùng nhầm bind mount:

```powershell
docker compose -p instacart-review --profile dashboard up -d --no-deps --wait --wait-timeout 180 dashboard
```

Lệnh vẫn dùng cổng 8501 nên chỉ một dashboard dùng cổng đó tại một thời điểm.

## 4. Chuẩn bị báo cáo và slide

Gửi ZIP `instacart-handoff-...zip` đã tạo để đối chiếu với số liệu terminal.
Báo cáo/slide cần thể hiện rõ:

1. Mục tiêu phân tích và bài toán hoàn thiện giỏ hàng Top 5.
2. Dữ liệu, kiến trúc một máy với Spark Master/Worker và HDFS.
3. Các tầng dữ liệu và bằng chứng kiểm tra chất lượng.
4. EDA toàn bộ dữ liệu, tách biệt với dữ liệu học.
5. Mẫu theo user, split không trùng user, cách tạo context/target và chống leakage.
6. FP-Growth, cách xếp hạng luật, Popularity và Hybrid.
7. Grid validation, mô hình đã chọn, kết quả test và giới hạn.
8. Dashboard, hướng dẫn demo và tái lập.

Kết luận phù hợp với kết quả hiện tại: pipeline và demo đã hoạt động; Hybrid có
cải thiện nhỏ so với Popularity trong lần chạy này, rule-only có coverage thấp.
Không kết luận Hybrid vượt trội hoặc hệ thống đã được kiểm chứng trên nhiều máy.
