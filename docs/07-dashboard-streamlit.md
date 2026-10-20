# Bước 7 — Dashboard Streamlit dùng đúng mô hình đã đánh giá

## Kết quả mô hình được giữ nguyên

Theo kết quả chạy bạn cung cấp: model run `20261020T131023Z-cae680de`, split run
`20261020T124751Z-e4b31e98`, support 0,002, confidence 0,40, 91 luật và 1.668
frequent itemsets. Model được chọn bằng 271 ca validation, chấm trên 306 ca test.

| Phương pháp | HitRate@5 test | Số ca có gợi ý trúng | Coverage test |
|---|---:|---:|---:|
| Popularity | 22,8758% | 70/306 | 100% |
| FP-Growth rule-only | 0,6536% | 2/306 | 6,8627% |
| Hybrid | 23,2026% | 71/306 | 100% |

Hybrid cao hơn Popularity khoảng 0,327 điểm phần trăm, chênh lệch ròng một ca
có gợi ý trúng. Chưa kết luận ưu thế tổng quát từ mẫu này. Rule-only dùng cấu
hình được chọn để tối ưu Hybrid, không phải một cấu hình được tối ưu riêng.
Coverage thấp cho thấy phần lớn ca Hybrid được phục vụ bằng Popularity.

Dashboard lấy số liệu từ các artifact đã chạy, không hardcode bảng kết quả này
vào ứng dụng. Không thay mô hình, retune hoặc dùng test để chọn tham số mới.

## 1. Thêm gói bước 7

Chép nội dung gói vào `D:\School\BigData\instacart-basket`, chấp nhận thay
compose.yaml. Cấu hình bốn dịch vụ Spark/HDFS của bản sửa ML được giữ nguyên;
chỉ thêm dịch vụ dashboard với profile riêng.

Gói có app Streamlit, Dockerfile/requirements cho app, script export, helper
kiểm tra checksum và kiểm thử. Không cài Streamlit vào image Spark hoặc lên
Python Windows; app có image riêng.

## 2. Export dữ liệu đã được kiểm chứng

Giữ bốn dịch vụ Spark/HDFS chạy rồi thực hiện:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/export_dashboard.py --model-run 20261020T131023Z-cae680de
```

Đợi dòng `DASHBOARD_EXPORT_PASS`.

Script xác nhận READY của mô hình, test result và preparation; kiểm tra đúng
model/checksum/protocol; đối soát các metric bằng dự đoán test đã lưu. Không
chạy suy luận mới trên test và không fit lại. Catalog tên sản phẩm và sáu bảng
Gold được đọc từ đúng preparation đã tạo ra sample của mô hình này.

Đầu ra:

```text
outputs/dashboard/latest.json
outputs/dashboard/20261020T131023Z-cae680de/
  manifest.json
  bundle.json
  serving.json
  catalog.csv
  eda/*.csv
```

Manifest chứa SHA-256 các tệp. Export được tạo đầy đủ trong thư mục tạm rồi đổi
tên và cập nhật pointer latest. Nếu export cùng model đã có, script kiểm tra
tính nhất quán trước khi dùng lại. Nó không sửa dữ liệu trên HDFS.
Nếu export lỗi thì dừng tại đó, gửi log; không khởi động app bằng dữ liệu chưa đạt.

## 3. Build và mở dashboard

```powershell
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --no-deps --wait --wait-timeout 180 dashboard
docker compose --profile dashboard ps dashboard
```

Mở trình duyệt tại:

http://localhost:8501

Chỉ cổng loopback của máy được mở. App đọc outputs và scripts qua mount read-only.
Sau export, dashboard không cần kết nối Spark/HDFS để hiển thị hoặc gợi ý.
Nếu đang trình diễn và không có job khác, có thể dừng bốn dịch vụ xử lý để giảm
bộ nhớ trong khi giữ app chạy:

```powershell
docker compose stop spark-master spark-worker namenode datanode
```

Mở lại các dịch vụ xử lý bằng `docker compose up -d --wait --wait-timeout 300`
khi cần chạy job mới. Giữ named volumes, không dùng down -v.

## 4. Năm trang

| Trang | Nội dung |
|---|---|
| Tổng quan | KPI toàn bộ dữ liệu, sản phẩm phổ biến và cấu hình thí nghiệm |
| Hành vi mua sắm | Giờ/ngày đặt hàng, kích thước giỏ, ngành hàng và reorder rate |
| Luật kết hợp | 91 luật, support/confidence/lift, lọc để xem và giải thích |
| Thử giỏ hàng | Tìm sản phẩm, chọn context, xem ba phương pháp cùng nguồn gợi ý |
| Đánh giá & tái lập | Validation/test, so sánh cặp Hybrid/Popularity, grid và lineage |

KPI và biểu đồ EDA dùng toàn bộ dữ liệu theo phạm vi ghi trên trang; demo dùng
Popularity và luật học từ 22.117 giỏ train. Hai nguồn có mục đích khác nhau,
không lấy bảng độ phổ biến Gold làm baseline cho demo.

Bộ lọc confidence ở trang luật chỉ thay phần hiển thị. Nó không thay bộ luật
của demo hoặc các metric đã công bố. Demo gọi chung recommendation_core.py
đã dùng trong bước đánh giá, giữ Top 5, loại context và trùng sản phẩm.

Nút ví dụ lấy antecedent của một luật có sẵn để minh họa. Nó được ghi nhãn rõ,
không trình bày như một ca test ngẫu nhiên hay bằng chứng về độ chính xác.

Coverage trên trang đánh giá là tỷ lệ nhận ít nhất một gợi ý. Chênh lệch điểm
được trình bày bằng điểm phần trăm; đếm số ca chỉ một phương pháp trúng dùng
dự đoán cũ của cùng các ca, không suy ra hai phương pháp chỉ khác nhau đúng một ca.

## 5. Kiểm tra trước khi gửi kết quả

- Sidebar chuyển được đủ năm trang.
- Tổng quan có 206.209 user, 3.421.083 orders và 3.346.083 giỏ có nhãn.
- Luật có 91 luật của mô hình, support 0,002, confidence 0,40.
- Demo chỉ gợi ý sản phẩm chưa nằm trong context; mỗi sản phẩm ghi nguồn luật/Popularity.
- Bảng test có 306 ca cho cả ba phương pháp, đúng các điểm đã ghi nhận.
- Trang đánh giá ghi rõ giới hạn một sample/seed/mask và chênh lệch nhỏ với baseline.

Nếu có lỗi:

```powershell
docker compose --profile dashboard logs --tail 100 dashboard
docker compose --profile dashboard ps dashboard
```

Gửi lỗi hoặc ảnh giao diện đang gặp vấn đề. Nếu trang báo checksum không khớp,
giữ nguyên bộ export để chẩn đoán; không sửa tay file metric để làm trang chạy.

## Kiểm thử và trạng thái xác minh

Đã chạy bốn kiểm thử Streamlit AppTest trên dữ liệu giả lập: đủ năm trang,
demo dùng shared engine, phát hiện export thay đổi, xử lý thiếu dữ liệu.
Kiểm thử đối soát dự đoán cũ cũng đã PASS, gồm từ chối metric sai hoặc thiếu ca.
Fixture được gắn nhãn synthetic và ứng dụng hiện cảnh báo nếu chạy bằng fixture.
Không dùng fixture làm số liệu Instacart hoặc dữ liệu mặc định của production.

Môi trường tạo gói chưa có Docker Engine và không có toàn bộ HDFS của máy bạn;
export thực tế và build app image được xác nhận bằng các lệnh trong hướng dẫn.
Không cần chạy lại train hoặc test để tạo dashboard.

Kiểm thử phát triển (tùy chọn, môi trường có dependencies app):

```powershell
python -m unittest discover -s tests -p test_dashboard.py
```

Kiểm thử logic export có thể chạy trong Spark container mà không mở SparkSession:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_dashboard_export.py
```

Lưu mã sau khi kiểm tra đạt:

```powershell
git add compose.yaml app scripts/dashboard_bundle.py scripts/export_dashboard.py scripts/test_dashboard_export.py tests/dashboard_fixture.py tests/test_dashboard.py docs/07-dashboard-streamlit.md
git commit -m "Add verified Streamlit dashboard using evaluated model artifacts"
```

Sau dashboard, các phần còn lại là kiểm tra tái chạy/đóng gói, hoàn thiện diễn
giải kết quả, báo cáo và slide từ số liệu của project mới.
