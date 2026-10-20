# Bước 6 — FP-Growth, Popularity, Hybrid và đánh giá

## Bộ chia đã đạt

Split run: `20261020T124751Z-e4b31e98`; cả 35 kiểm tra PASS, 24,578 giây.

| Thành phần | Người dùng | Dữ liệu dùng |
|---|---:|---|
| Train | 1.412 | 22.117 giỏ prior, 17.651 sản phẩm quan sát được |
| Validation | 281 | 271 ca hợp lệ, loại 10 giỏ một sản phẩm |
| Test | 324 | 306 ca hợp lệ, loại 18 giỏ một sản phẩm |

User ba tập không giao nhau. Unseen targets được giữ: validation có 143 lượt
target chưa gặp trên 1.624 lượt target; test có 100 trên 1.677. Số liệu này mô tả
dữ liệu đánh giá, chưa phải kết quả mô hình và không dùng để chọn tham số từ test.

## 1. Thêm mã

Chép nội dung gói step06 vào `D:\School\BigData\instacart-basket`:

```text
scripts/recommendation_core.py
scripts/train_validate.py
scripts/evaluate_test.py
scripts/test_model_pipeline.py
tests/test_recommendation_core.py
docs/06-huan-luyen-va-danh-gia.md
```

Dùng lại helper ingest_bronze.py và split_evaluation.py. Không đổi Docker image,
không upload lại dữ liệu hoặc chia lại user. Thư mục scripts đã được mount sẵn.

## 2. Huấn luyện và chọn cấu hình trên validation

PowerShell:

```powershell
Set-Location D:\School\BigData\instacart-basket
docker compose up -d --wait --wait-timeout 300
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/train_validate.py --split-run 20261020T124751Z-e4b31e98
```

Đợi log cuối:

```text
MODEL_VALIDATION_PASS run_id=...
```

Nếu lệnh lỗi, dừng tại đó, gửi traceback cuối và không chạy test bằng một latest.json
cũ. Một run mới được tạo khi chạy train_validate; không ghi đè run mô hình trước.

Script thực hiện đúng protocol SHA-256 đã chốt ở bước 5:

1. Đọc train_baskets và validation_cases của đúng split run.
2. Đối soát số giỏ/user, nguồn prior và không trùng user giữa train và validation.
3. Tính Popularity bằng số giỏ train có chứa mỗi sản phẩm.
4. Fit Spark FP-Growth cho ba mức minSupport 0,001 / 0,002 / 0,005.
5. Với mỗi mức support, thử minConfidence 0,20 / 0,40 / 0,60, tổng 9 cấu hình.
6. Chọn bằng Hybrid HitRate@5 trên validation, theo tie-break đã ghi ở protocol.
7. Lưu và nạp lại mô hình Spark được chọn, kiểm tra điểm validation tái tạo được.
8. Lưu _SELECTION.json và bộ serving có checksum rồi công bố _READY.json.

minConfidence không làm thay đổi việc khai phá frequent itemsets, nên chỉ cần
ba lần fit theo minSupport, không cần fit lại cùng support cho mỗi confidence.
Những phép lọc confidence cho grid dùng đúng thống kê luật mà Spark xuất ra.

Job này không đọc bảng test_cases và không chấm điểm test.
FP rule-only và Hybrid dùng chung cấu hình đã chọn bằng điểm Hybrid, như protocol
đã chốt. Không diễn giải FP rule-only là đã tối ưu riêng.

Xem kết quả validation:

```powershell
$fit = Get-Content .\outputs\models\latest.json -Raw | ConvertFrom-Json
$fit | Select-Object status, run_id, source_split_run, train_baskets, vocabulary_products, duration_seconds
$fit.selected | ConvertTo-Json
$fit.validation_metrics | ConvertTo-Json -Depth 5
$fit.checks | Where-Object { $_.pass -eq $false }
```

Kỳ vọng số giỏ train 22.117, số sản phẩm train 17.651 và mỗi phương pháp có 271
ca validation. Chưa biết trước cấu hình thắng hoặc phương pháp nào có điểm cao nhất.
Nếu Hybrid không hơn baseline, báo cáo đúng kết quả đó.

## 3. Chạy test bằng mô hình đã chọn

Chỉ thực hiện sau MODEL_VALIDATION_PASS của lệnh vừa chạy.

```powershell
$fit = Get-Content .\outputs\models\latest.json -Raw | ConvertFrom-Json
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/evaluate_test.py --model-run $fit.run_id
```

Kết thúc cần có:

```text
TEST_EVALUATION_PASS split_run=... model_run=...
```

Script đọc bộ serving đã đóng băng, kiểm tra checksum/nguồn/protocol, gắn model
vào test result của split này rồi mới đọc test_cases. Không fit lại mô hình,
không đổi cấu hình, không tính Popularity bằng dữ liệu test.

Một split được gắn với một model run đã chấm test. Gọi lại cùng mô hình sau khi
đã hoàn tất sẽ trả lại báo cáo lưu sẵn với `reused=true`, không chấm lại. Một model
run khác sẽ bị từ chối nếu split này đã có test result. Nếu lần test bị lỗi giữa
chừng, giữ dấu vết và gửi log để xử lý; không tự xóa thư mục test-results hoặc
tạo split mới nhằm vượt qua kiểm tra này.

Cơ chế này bảo vệ quy trình chạy mặc định; nó không phải cơ chế kiểm soát truy
cập chống người cố tình sửa code, đổi split hay xóa dữ liệu. Protocol và nhật ký
thực nghiệm vẫn là căn cứ để diễn giải các lần chạy.

Đọc kết quả:

```powershell
$test = Get-Content .\outputs\test\latest.json -Raw | ConvertFrom-Json
$test | Select-Object status, source_model_run, source_split_run, duration_seconds
$test.selected | ConvertTo-Json
$test.test_metrics | ConvertTo-Json -Depth 5
$test.checks | Where-Object { $_.pass -eq $false }
```

Mỗi phương pháp phải có 306 ca test. Lệnh lọc checks không in gì khi đạt.
Gửi phần tóm tắt model, selected, validation_metrics và test_metrics.

## 4. Quy tắc gợi ý và ý nghĩa chỉ số

Mọi phương pháp chỉ nhận context. Target được đưa vào hàm chấm điểm riêng.
Rule-only yêu cầu đủ toàn bộ antecedent, loại sản phẩm có trong context, chọn
luật tốt nhất cho từng consequent rồi xếp confidence/lift/support giảm dần,
product_id tăng dần. Nếu cùng sản phẩm và mọi điểm bằng nhau, thứ tự antecedent
chỉ dùng ổn định phần giải thích, không thay đổi thứ hạng sản phẩm.

Hybrid giữ phần đầu từ luật rồi điền Popularity vào các vị trí thiếu; không
cắt cố định top-5 Popularity trước bước loại trùng vì như vậy có thể làm hụt
ứng viên bổ sung. Khi không có luật phù hợp, rule-only có thể trả danh sách rỗng.

| Trường kết quả | Ý nghĩa |
|---|---|
| cases | Tất cả ca đủ điều kiện, gồm ca không có gợi ý |
| hit_rate_at_5 | Tỷ lệ ca có ít nhất một sản phẩm trúng target |
| precision_at_5 | Trung bình số trúng chia 5, kể cả khi danh sách ngắn |
| recall_at_5 | Trung bình số trúng chia số target của từng ca |
| ndcg_at_5 | Điểm trúng theo thứ hạng, chuẩn hóa theo target |
| coverage_at_5 | Tỷ lệ ca có ít nhất một gợi ý |
| full_list_rate_at_5 | Tỷ lệ ca nhận đủ 5 gợi ý |

Các giá trị tỷ lệ nằm trong 0–1; nhân 100 nếu trình bày dưới dạng %. Coverage cao
không đồng nghĩa gợi ý đúng. Những sản phẩm target chưa gặp vẫn nằm trong mẫu số.

## 5. Artifact và nơi lưu

HDFS của model run:

```text
/instacart/models/<model_run>/
  popularity/
  candidates/support_*/spark_model/
  candidates/support_*/base_rules/
  selected_model/
  _SERVING.json
  _SELECTION.json
  _REPORT.json
  _READY.json
```

Trên Windows:

```text
outputs/models/<model_run>/report.json
outputs/models/<model_run>/serving.json
outputs/models/<model_run>/grid.json
outputs/models/<model_run>/validation_predictions.json
outputs/models/<model_run>/validation_metrics.csv
outputs/models/latest.json
```

Kết quả test:

```text
/instacart/test-results/<split_run>/_REPORT.json
/instacart/test-results/<split_run>/_PREDICTIONS.json
/instacart/test-results/<split_run>/_READY.json
outputs/test/<split_run>/report.json
outputs/test/<split_run>/predictions.json
outputs/test/<split_run>/test_metrics.csv
outputs/test/latest.json
```

Model report giữ test_status=NOT_EVALUATED tại thời điểm chọn cấu hình; kết quả
test được lưu riêng, không sửa lại lịch sử lựa chọn mô hình.

serving.json chứa Popularity và bộ luật đúng cấu hình đã đánh giá; đây là đầu
vào cho demo/dashboard sau này, không dùng một mô hình khác huấn luyện trên cả
validation/test rồi gắn nhãn là mô hình đã đánh giá.

## 6. Phạm vi tính toán và giới hạn hiện tại

Spark thực hiện pipeline dữ liệu, tính frequency và fit FP-Growth. Bộ luật được
export có giới hạn về driver Python để xếp hạng và chấm mẫu vài trăm giỏ; không
gọi phần chấm điểm này là inference phân tán. Dashboard sau sẽ dùng chung core.

Nếu một cấu hình tạo hơn 50.000 luật, chương trình dừng trước collect. Không tự
lọc bớt luật hoặc bỏ cấu hình khỏi grid; gửi log để điều chỉnh cách thực thi có
ghi nhận. Giới hạn này nhằm giữ bộ export phù hợp máy hiện tại.

Thời gian fit được báo gồm fit và materialize frequent itemsets. Tổng thời gian
model run còn gồm I/O, grid validation và export. Thời gian test báo trong script
không gồm khởi tạo SparkSession. Không so trực tiếp các số đó như benchmark hay
suy ra tăng tốc cụm nhiều máy. Bài toán vẫn là basket completion với một mẫu,
một seed chia user và một lần mask; tập test 306 ca còn nhỏ.

## Kiểm thử phát triển

Core thuần Python, không cần Docker:

```powershell
python -m unittest discover -s tests -p test_recommendation_core.py
```

Spark với dữ liệu giả lập, tùy chọn và không chạy đồng thời với job khác:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/test_model_pipeline.py
```

Kiểm thử core kiểm tra đủ antecedent, thứ hạng, loại trùng/context, fallback,
metric tính tay, mẫu số có ca trượt/unseen target và tie-break cấu hình. Kiểm thử
Spark chạy thật ba lần fit, chín cấu hình, save/load, thống kê luật đã tính tay
và tái tạo điểm bằng serving JSON. Dữ liệu giả lập không phải kết quả Instacart.

Trạng thái xác minh khi tạo gói: 9 unit tests của core đã đạt; Spark 3.5.7,
Java 17, local[2] đã chạy đạt toàn bộ kiểm thử fit/grid/save/load/serving trên
filesystem local. Các script điều phối đã kiểm tra cú pháp. Chưa chạy huấn luyện
và chấm test thực tế trên bộ HDFS của máy bạn; kết quả cần lấy từ các lệnh ở trên.

Sau khi hoàn tất:

```powershell
git add scripts/recommendation_core.py scripts/train_validate.py scripts/evaluate_test.py scripts/test_model_pipeline.py tests/test_recommendation_core.py docs/06-huan-luyen-va-danh-gia.md
git commit -m "Train FP-Growth select on validation and evaluate frozen test model"
```

## Nguồn kỹ thuật

- https://spark.apache.org/docs/3.5.7/ml-frequent-pattern-mining.html
- https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.fpm.FPGrowthModel.html
