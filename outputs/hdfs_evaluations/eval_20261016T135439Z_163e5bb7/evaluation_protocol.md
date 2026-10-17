# Giao thức khóa trước đánh giá — basket completion v1

Bài toán: bổ sung sản phẩm vào một giỏ có nhãn quan sát được. Không gọi đây
là dự đoán đơn tiếp theo. Dataset là ETL sample theo order_id bucket, lịch sử
user có thể thiếu; không đại diện toàn bộ Instacart. Catalog dùng chung chỉ
để ánh xạ ID/tên; mọi frequency/popularity tính riêng từ training baskets.

Seed cố định 20261016; K=5. User bucket là int(first 8 hex của
SHA256(`instacart-user-v1:<user_id>:20261016`),16) % 10000. Train <7000,
validation 7000..8499, test >=8500. Không dùng Python hash(). User được chia
từ toàn bộ Silver orders; cohort đánh giá chỉ gồm user có giỏ có nhãn >=2.

Fit chỉ prior baskets của train users, unique integer items, >=2 items.
Mỗi validation/test user chọn một giỏ >=2: ưu tiên eval_set=train của
Instacart, rồi order_number giảm, order_id giảm; nếu thiếu thì prior quan sát
cuối. Không sử dụng eval_set=test vì không có product labels. 'train-order'
chỉ là tên nguồn Instacart, không phải split train của thí nghiệm này.

Sắp items bằng SHA256(`instacart-item-v1:<user_id>:<order_id>:<product_id>:20261016`)
rồi product_id. Context lấy floor(n/2), tối thiểu 1 và tối đa n-1; target là
phần còn lại. Giữ target hiếm/cold và các case không có recommendation.
Context/target lưu thành bảng riêng cho validation và test. Predictor chỉ
nhận context; evaluator mới join targets sau khi predictions đã được lưu.

Grid cố định supports [0.002, 0.001], confidences [0.20,0.30,0.40]; một fit
mỗi support, sinh rules ở confidence 0.20. Spark local[2], task.cpus=1,
driver 1g, partitions=8; worker timeout 1200s, caps itemsets 200000/rules
500000, giới hạn engine rules 20000 và popularity 100000 trước broadcast.
Các giới hạn không ngăn OOM trong fit; mỗi fit ở process group riêng.
FAIL/OOM/TIMEOUT được ghi riêng, không coi là cấu hình có metrics bằng 0.

Selection dùng **validation Recall@5 của rule-only**, rồi HitRate@5,
rồi support cao hơn, confidence cao hơn, config ID để phá hòa. Hai tiêu chí
sau ưu tiên cấu hình ít pattern/luật theo tham số, không dùng thời gian đo
ngẫu nhiên hay test để chọn. Không refit trên validation sau selection.
Ghi locked_config.json và fingerprints trước khi đo test. Test chạy một
lần cho lock này; nếu sửa mô hình sau khi xem test phải ghi test đã dùng
để phát triển, không giữ nhãn test chưa sử dụng.

Ba phương pháp trên cùng context/target và cùng K:

- Popularity: số training baskets chứa item, giảm dần; product_id tăng dần.
- Rule-only: đủ antecedent, loại context, distinct product; luật tốt nhất
  theo confidence/lift/support giảm dần, product_id tăng dần; rule_id phá
  hòa cùng product/score để explanation ổn định. Chỉ lift>1/single consequent.
- Hybrid: lấy rule-only trước; thiếu 5 thì bổ sung train popularity, loại
  context và mọi ID đã chọn. Ghi nguồn rule/popularity cho từng sản phẩm.

Macro theo case (mỗi eligible user đúng một case): HitRate@5 = mean(hits>0),
Precision@5 = mean(hits/5), Recall@5 = mean(hits/target_size).
Recommendation coverage = mean(n_recommendations>0), rule coverage =
mean(n_rule_recommendations>0). Báo denominator, zero cases, <5 cases và số
gợi ý từ mỗi nguồn. Không thêm catalog coverage vào giao thức này.
Cold target exposure = số target occurrences không có trong train popularity
/ tổng target occurrences; chỉ báo riêng, không lọc target khỏi phép đo.

Predictions và per-case scores lưu HDFS; metrics summary phải được tính lại
từ per-case readback. Báo thời gian job Spark batch riêng. Latency nếu đo
là warm in-process engine, model đã nạp, tối đa 100 test contexts theo
order_id, mỗi case 3 lần/method sau một warm-up, perf_counter; p50/p95 từ
các lần gọi thực, không lấy thời gian batch chia số case.

Validation/test CSV và biểu đồ tách riêng. Dashboard evaluation dùng đúng
selected refit model và train popularity; pilot Task 01–03 giữ nguyên.
Mọi checkpoint và artifacts bất biến; không xóa kết quả để chạy lại.
