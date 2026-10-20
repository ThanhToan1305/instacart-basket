# Lộ trình từ đầu đến cuối

## Giai đoạn 1 — Khởi tạo sạch

Tạo repository riêng, kiểm tra máy, tải sáu CSV Instacart. Giữ dữ liệu gốc nguyên
trạng. Hoàn thành khi Git hoạt động, Docker server sẵn sàng và header CSV hợp lệ.
Bộ khung hiện tại thực hiện phần tổ chức thư mục và kiểm tra đầu vào của bước này.

## Giai đoạn 2 — Môi trường chạy tái lập

Chọn và cố định bộ phiên bản Python, Java, Spark, Hadoop tương thích sau khi xác
minh tài liệu và image thực tế. Xây image worker cho Spark và Compose cho HDFS.
Ban đầu dùng một NameNode, một DataNode và Spark local[2] trên Docker Desktop.
Ghi rõ đây là lưu trữ HDFS với tính toán local trên một máy.

Nghiệm thu: container khỏe, Spark đếm được dữ liệu mẫu, HDFS ghi/đọc Parquet được,
khởi động lại không mất dữ liệu đã lưu. Không dùng latest cho cấu hình nghiệm thu.

## Giai đoạn 3 — Quy ước dữ liệu và ETL

Khai báo schema tường minh cho sáu CSV. Kiểm tra null bắt buộc, duplicate keys,
miền giá trị, eval_set và khóa ngoại. days_since_prior_order có thể null ở đơn đầu.
Không tạo nhãn giả cho orders eval_set=test không có chi tiết sản phẩm.

Raw: nguồn. Bronze: dữ liệu ingest và metadata. Silver: dữ liệu chuẩn hóa, nối
bảng và baskets. Gold: KPI, catalog và các đầu ra dành cho sử dụng.
Gom product_id duy nhất theo order_id, giữ user_id để chia tập và chống rò rỉ.

Dùng sample xác định để phát triển, ghi rõ sample chọn theo user hay order; ưu tiên
sample theo user nếu cần giữ lịch sử. Lưu seed, danh sách ID và cấu hình sample.
Đọc lại đầu ra sau ghi, đối soát số dòng và tổng số item trước/sau tạo giỏ.

Nghiệm thu: có report chất lượng, count và schema từng tầng, job lỗi phải FAIL.

## Giai đoạn 4 — Phân tích hành vi

Tính người dùng, đơn, giỏ có nhãn, sản phẩm, kích thước giỏ, reordered, phân bố giờ,
ngày trong tuần, sản phẩm/aisle/department phổ biến. Xuất aggregate để vẽ chart.
Phân biệt số đơn với số dòng sản phẩm; không diễn giải tần suất thành doanh thu.

Nghiệm thu: mỗi bảng/biểu đồ có nguồn, đơn vị và phạm vi sample/full rõ ràng.
EDA toàn dữ liệu không được dùng làm popularity cho đánh giá độc lập.

## Giai đoạn 5 — Khóa giao thức đánh giá

Bài toán chính: basket completion. Chia user thành train/validation/test rời nhau
bằng hash và seed cố định. Chỉ dùng giỏ prior đủ điều kiện của train users để học.
Với user validation/test, chọn một giỏ có nhãn theo quy tắc cố định rồi chia item
thành context/target, không rỗng, không giao, hợp lại đúng giỏ gốc.

Chốt tỷ lệ chia, tỷ lệ ẩn item, cách chọn giỏ, K=5, tie-break và metric trước khi
chạy mô hình. Chỉ predictor được thấy context; target dành cho scorer.
Đây là gợi ý hoàn thiện giỏ, chưa phải dự đoán đơn tương lai.

Nghiệm thu: test split disjoint, không rò target, split manifest được lưu.

## Giai đoạn 6 — FP-Growth và baseline

Học frequent itemsets trên giỏ có ít nhất hai sản phẩm. Sinh luật consequent một
sản phẩm. Bắt đầu grid support nhỏ vừa phải, confidence 0.20/0.30/0.40; điều chỉnh
phạm vi dựa trên tài nguyên trước khi khóa test. Không mang kết quả cũ sang.

Popularity: xếp theo số giỏ training chứa sản phẩm, loại context.
Rule-only: antecedent phải nằm trọn trong context, loại context và kết quả trùng;
chốt thứ tự confidence, lift, support và product_id để tái lập.
Hybrid: ưu tiên luật, bổ sung popularity đến tối đa năm sản phẩm.
Giữ source của từng gợi ý; không gán thông số luật cho popularity.

Nghiệm thu: kiểm thử antecedent đầy đủ, loại sản phẩm đã có, deduplicate và thứ tự
ổn định. Đo quality trên validation rồi khóa model/config trước test.

## Giai đoạn 7 — Đánh giá cuối

So sánh ba phương pháp trên cùng ca: HitRate@5, Precision@5, Recall@5,
recommendation coverage và rule coverage. Precision chia cho 5 kể cả khi trả ít.
Ca rỗng vẫn nằm trong mẫu số; Recall macro trung bình theo ca.
Lưu prediction và per-case scores; tái tính summary từ dữ liệu đã ghi.

Đo thời gian ETL, fit, scoring riêng. Đo warm engine và UI end-to-end riêng.
Không tuyên bố HDFS tăng tốc khi chưa có benchmark cùng đầu vào/tài nguyên.
Nếu đủ tài nguyên, chạy nhiều seed với giao thức đã xác định; tránh chỉnh theo test.

Nghiệm thu: bảng baseline thực tế, run ID, config và giới hạn kết luận rõ ràng.

## Giai đoạn 8 — Dashboard

Năm trang: Tổng quan, Hành vi mua sắm, Luật kết hợp, Gợi ý sản phẩm, Hiệu năng.
Dashboard chỉ đọc serving bundle đã export; không huấn luyện mỗi lần tương tác.
Hiển thị chế độ sample/full, nguồn model, run, ngày chạy và metric đúng bundle.
Dùng chung engine với evaluation. Chụp ảnh runtime sau khi chạy thành công.

Nghiệm thu: chọn giỏ, nhận gợi ý, xem nguồn luật, metric trùng report.

## Giai đoạn 9 — Mở rộng dữ liệu và đóng gói

Chạy sample trước, tăng dần khi tài nguyên cho phép. Nếu full chưa chạy thành công,
báo rõ giới hạn; không suy rộng sample thành full. Lưu log lỗi và cấu hình.
Viết lệnh chạy cho từng job; kiểm tra từ checkout mới, cần data/artifact nào ghi rõ.
Không commit CSV lớn, secrets, HDFS volumes hoặc model nặng vào Git.

## Giai đoạn 10 — Báo cáo và thuyết trình

Viết sáu chương: Giới thiệu; Cơ sở lý thuyết; Dữ liệu và kiến trúc; Triển khai;
Thực nghiệm; Kết luận. Dùng số liệu/ảnh của project mới. Ghi rõ cấu hình máy,
phiên bản, dataset, split, model và phạm vi mỗi số đo.

Chuẩn bị slide và kịch bản demo: kiến trúc → dữ liệu → EDA → luật → gợi ý → đánh
giá và hạn chế. Chỉ kết luận những gì kết quả thực nghiệm hỗ trợ.
