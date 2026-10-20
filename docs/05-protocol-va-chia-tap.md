# Bước 5 — Chốt protocol và chia dữ liệu đánh giá

## Kết quả bước 4 đã xác nhận

Preparation run: `20261020T123447Z-5ba5babb`, 24 đối soát PASS, 92,228 giây.

| Chỉ tiêu | Kết quả trong log |
|---|---:|
| Giỏ có nhãn toàn bộ dữ liệu | 3.346.083 |
| Lượt sản phẩm | 33.819.106 |
| Kích thước giỏ trung bình | 10,1071 |
| Kích thước giỏ nhỏ nhất / lớn nhất | 1 / 145 |
| Tỷ lệ lượt sản phẩm có reordered=1 | 59,0062% |
| Người dùng trong mẫu | 2.017 |
| Đơn trong mẫu, kể cả test thiếu nhãn | 34.177 |
| Giỏ có nhãn trong mẫu | 33.454 |
| Giỏ prior / train gốc trong mẫu | 32.160 / 1.294 |

Tỷ lệ user thực tế 0,9781%, gần mức yêu cầu 1%. Đây là kết quả của quy tắc hash,
không phải lỗi thiếu người dùng. Kiểm tra lịch sử hai chiều đã PASS.
reordered là cờ ở cấp lượt sản phẩm, không phải tỷ lệ khách hàng quay lại.

## 1. Mục tiêu đánh giá

Đề tài gợi ý sản phẩm mua kèm được đánh giá bằng basket completion:
lấy một giỏ có nhãn, cung cấp một phần làm context và ẩn phần còn lại làm target.
Hệ thống gợi ý tối đa 5 sản phẩm chưa có trong context; đối chiếu với target.

Ví dụ minh họa, không phải kết quả thực nghiệm:

```text
Giỏ quan sát được: [sữa, bánh mì, trứng, chuối]
Context:           [sữa, bánh mì]
Target:            [trứng, chuối]
```

Protocol này đo hoàn thiện một giỏ đã quan sát được. Nó không chứng minh dự đoán
đơn tương lai. Với user có đơn cuối eval_set=test nhưng không có nhãn sản phẩm,
giỏ đánh giá sẽ là giỏ có nhãn gần nhất trước đó; không tự tạo nhãn cho đơn test.

## 2. Phân biệt hai cách dùng train/test

| Thuật ngữ | Ý nghĩa |
|---|---|
| eval_set của Instacart | prior/train/test gốc trong orders.csv |
| split của nghiên cứu | train/validation/test do project chia theo user |

Một user thuộc split=test của nghiên cứu vẫn có thể có giỏ eval_set=train của
Instacart. Đây là hai khái niệm khác nhau. Script giữ cả split và source_eval_set
trong các ca đánh giá để truy vết rõ ràng.

## 3. Chia user trước khi học

Dùng 2.017 user của sample. Chia theo bucket hash với seed=42:

```text
bucket = pmod(xxhash64('user-split-v1', seed_long, user_id_long), 10000)
0..6999:    train
7000..8499: validation
8500..9999: test
```

Tỷ lệ mục tiêu là 70/15/15; số user thực tế được tính khi chạy. Không ép số lượng
bằng cách cắt hoặc chuyển user sau khi xem kết quả. Chuỗi domain 'user-split-v1'
giúp phép chia dùng đầu vào hash khác với phép chọn mẫu ở bước trước.

Mô hình FP-Growth và Popularity chỉ học từ giỏ eval_set=prior của train users.
Các giỏ eval_set=train gốc của train users chưa được dùng để fit trong protocol này.
Không dùng bất kỳ giỏ nào của validation/test users để tính frequency, luật,
support, confidence hoặc baseline. Các bảng Gold toàn bộ dữ liệu chỉ dùng cho EDA.

## 4. Chọn và che giỏ đánh giá

Với từng user thuộc validation hoặc test:

1. Chọn giỏ có nhãn gần nhất theo order_number giảm dần, tie-break order_id giảm dần.
2. Nếu giỏ chỉ có một sản phẩm, ghi nhận bị loại vì không đủ context và target.
   Không thay bằng giỏ cũ hơn. Nếu không có giỏ có nhãn, ghi lý do riêng.
3. Với giỏ đủ điều kiện, sắp sản phẩm theo hash của domain 'basket-mask-v1', seed,
   user_id, order_id, product_id; tie-break product_id.
4. Lấy floor(kích_thước_giỏ/2) sản phẩm làm context, còn lại làm target.
5. Sắp context và target theo ID để lưu biểu diễn ổn định.

Ví dụ kích thước 2 cho context/target là 1/1; kích thước 5 cho 2/3.
Mỗi user tối đa một ca đánh giá. Context và target đều phải khác rỗng,
không giao nhau, và hợp của chúng phải khôi phục đúng giỏ gốc.

Sản phẩm trong target chưa từng có ở dữ liệu học vẫn được giữ khi chấm điểm.
Không lọc chúng đi để tăng chỉ số. Báo cáo ghi số lượt target chưa xuất hiện
và số ca chứa chúng. Các thống kê này không được dùng chọn tham số từ test.

## 5. Các phương pháp và cách chọn cấu hình, chốt trước khi có điểm

Ba phương pháp sẽ được triển khai ở bước tiếp theo:

| Phương pháp | Quy tắc |
|---|---|
| Popularity | Frequency theo giỏ train giảm dần, tie-break product_id tăng dần |
| FP-Growth rule-only | Luật có antecedent nằm trọn trong context; loại consequent đã có trong context |
| Hybrid | Lấy gợi ý luật trước, bổ sung Popularity đến tối đa 5 sản phẩm |

Tất cả danh sách gợi ý phải không trùng và không chứa sản phẩm đã có trong context.
Luật được xếp confidence giảm dần, lift giảm dần, support giảm dần, product_id tăng dần.
Khi nhiều luật cùng đề xuất một sản phẩm, giữ luật xếp cao nhất của sản phẩm đó.
Chưa thêm ngưỡng lift hoặc bộ lọc ngoài các quy tắc đã ghi ở đây.

Grid dự kiến:

- minSupport: 0,001; 0,002; 0,005.
- minConfidence: 0,20; 0,40; 0,60.

Có 9 tổ hợp. Chọn cấu hình bằng HitRate@5 của Hybrid trên validation. Khi hòa,
ưu tiên Recall@5 cao hơn, số luật ít hơn, minSupport cao hơn, minConfidence cao hơn.
FP rule-only và Hybrid dùng chung bộ luật của cấu hình đã chọn; rule-only không
được tuyên bố là đã được tối ưu độc lập. Popularity dùng cùng nguồn train và
cùng ca đánh giá để làm đối chứng.

Chốt cấu hình rồi mới chạy test. Không điều chỉnh tham số dựa vào điểm test;
nếu sau này thay protocol hoặc grid, ghi thành thí nghiệm mới với version riêng.
Kết quả test của mẫu và seed này chưa đủ để khẳng định ưu thế tổng quát.

## 6. Chỉ số ở K=5

Tính trên tất cả ca đủ điều kiện, gồm cả ca không có gợi ý hoặc target chưa gặp.

| Chỉ số | Định nghĩa |
|---|---|
| HitRate@5 | Tỷ lệ ca có ít nhất một gợi ý trúng target |
| Precision@5 | Trung bình số gợi ý trúng chia 5, kể cả khi trả ít hơn 5 |
| Recall@5 | Trung bình số gợi ý trúng chia số sản phẩm trong target |
| NDCG@5 | Điểm trúng có trọng số 1/log2(rank+1), chuẩn hóa bằng thứ hạng lý tưởng |
| Coverage@5 | Tỷ lệ ca nhận được ít nhất một gợi ý |
| FullListRate@5 | Tỷ lệ ca nhận đủ 5 gợi ý |

Coverage=100% nghĩa là ca nào cũng nhận được gợi ý, không phải ca nào cũng gợi ý đúng.
Trọng số lý tưởng của NDCG gồm min(5, số target) vị trí. Dùng nhãn relevance nhị phân.
Không đổi mẫu số Precision thành độ dài danh sách thực tế khi so sánh với rule-only.

## 7. Thêm mã và chạy bước này

Chép nội dung gói step05 vào project:

```text
scripts/split_evaluation.py
scripts/test_evaluation_split.py
docs/05-protocol-va-chia-tap.md
```

Script dùng helper ingest_bronze.py đã có. Chưa huấn luyện hoặc chấm điểm mô hình
trong bước này. Protocol được lưu ra JSON và SHA-256 của JSON canonical để
bước mô hình nhận diện đúng protocol đã chốt.

PowerShell:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose up -d --wait --wait-timeout 300
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/split_evaluation.py --preparation-run 20261020T123447Z-5ba5babb --seed 42
```

Đợi cuối log có:

```text
EVALUATION_SPLIT_PASS run_id=...
```

## 8. Đầu ra và nghiệm thu

HDFS:

```text
/instacart/evaluation/<run_id>/
  _PROTOCOL.json
  _REPORT.json
  _READY.json
  tables/user_splits/
  tables/train_baskets/
  tables/train_vocabulary/
  tables/case_eligibility/
  tables/validation_cases/
  tables/test_cases/
```

Không có model, luật hoặc điểm gợi ý ở bước này. _READY chỉ xuất hiện sau các
kiểm tra tách user, nguồn fit prior, bảo toàn train baskets, ca đánh giá hợp lệ,
không trùng user/order giữa các tập và đối soát số ca/loại trừ.

Trên Windows:

```text
outputs/splits/latest.json
outputs/splits/<run_id>/report.json
outputs/splits/<run_id>/protocol.json
```

Xem và gửi kết quả:

```powershell
$split = Get-Content .\outputs\splits\latest.json -Raw | ConvertFrom-Json
$split | Select-Object status, run_id, source_preparation_run, duration_seconds
$split.user_counts | ConvertTo-Json
$split.train | ConvertTo-Json
$split.case_counts | ConvertTo-Json -Depth 6
$split.checks | Where-Object { $_.pass -eq $false }
```

Lệnh cuối không in gì khi đạt. Nếu lần chạy mới lỗi, đọc report.json theo run_id
được in trong log, vì latest.json chỉ cập nhật sau lần thành công.
Giữ nguyên run đã tạo; không cần chạy lại các tầng Raw/Bronze/Silver.

Sau khi đạt, lưu mã:

```powershell
git add scripts/split_evaluation.py scripts/test_evaluation_split.py docs/05-protocol-va-chia-tap.md
git commit -m "Freeze basket completion protocol and split users without leakage"
```

Kiểm thử phát triển tùy chọn, không chạy đồng thời với job khác:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_evaluation_split.py
```

Kiểm thử dùng user/giỏ giả lập; kiểm tra tách user, không học nhãn train gốc,
giữ unseen targets, không fallback giỏ một item, mask ổn định theo thứ tự và
partition, đồng thời phát hiện context/target bị trùng. Đây không phải kết quả
chất lượng gợi ý trên Instacart.

Trạng thái xác minh khi tạo gói: chạy đạt trên Spark 3.5.7, Java 17, local[2],
filesystem local. Có 35 kiểm tra trong luồng tạo bộ chia, cùng kiểm tra độc lập
về mask ổn định, ca một item, thiếu giỏ có nhãn, target chưa gặp và phát hiện
context/target trùng. Chưa chạy tích hợp bộ chia trên sample HDFS của máy bạn.

## Nguồn kỹ thuật

- https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.fpm.FPGrowth.html
