# Checklist demo prototype — 6 phút

## Trước buổi demo

- [ ] Chạy từ thư mục gốc project với `.venv`, Java 17 và đủ sáu file trong `data/raw`.
- [ ] `chmod +x run_pipeline.sh` rồi `./run_pipeline.sh --resume`; kiểm tra `prototype_summary.json` có `status: PASS`, xem `logs/integrated_tests.log` kết thúc `OK`.
- [ ] Không chạy `--no-resume` trên workspace đã có đầu ra; runner sẽ từ chối.
- [ ] Mở Chương 5 (tài liệu lịch sử hiện không có trong workspace) và [model_comparison.csv](../outputs/metrics/model_comparison.csv).
- [ ] Khởi động dashboard nếu chưa chạy:

```bash
.venv/bin/python -m streamlit run app/dashboard.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false
```

- [ ] Mở http://127.0.0.1:8501; kiểm tra `curl --fail http://127.0.0.1:8501/_stcore/health` trả `ok`.

## Kịch bản 6 phút

| Mốc thời gian | Thao tác | Điểm cần trình bày |
|---|---|---|
| 0:00–0:40 | Giới thiệu kiến trúc raw → Bronze → Silver → Gold → dashboard | Spark chuẩn hóa và khai phá offline; dashboard chỉ đọc đầu ra nhỏ, không chạy pipeline khi mở. |
| 0:40–1:30 | Trang Tổng quan | 206.209 người, 3.421.083 đơn, 33.819.106 lượt mua, 49.688 sản phẩm. Đơn test không có chi tiết; tỷ lệ reorder 59,0062%. |
| 1:30–2:20 | Trang Hành vi mua sắm | Giờ đỉnh 10; mã ngày giữ 0–6, không gán tên thứ. Banana đứng đầu; giải thích trung bình giỏ 10,1071 và trung vị 8. |
| 2:20–3:20 | Trang Luật kết hợp | Cấu hình chọn support 0,001/confidence 0,20, 834 luật. Tăng confidence/lift, quan sát bảng và scatter, tải CSV. Đặt minSupport=1 để minh họa trạng thái không có kết quả; trả về 0,001. |
| 3:20–4:35 | Trang Gợi ý sản phẩm | Chọn Hass Avocados (#12341), quan sát Top-5 và luật nguồn. Thêm một sản phẩm vừa được gợi ý: sản phẩm đó phải biến mất khỏi danh sách. Xóa giỏ để thấy hướng dẫn; chọn sản phẩm không thuộc antecedent để minh họa fallback nếu cần. Fallback hiển thị rõ nguồn và không có confidence/lift giả. |
| 4:35–5:20 | Trang Hiệu năng hệ thống | Stage, số dòng vào/ra, cấu hình Spark/FP-Growth, 9 tổ hợp; 0,0005 bị bỏ qua theo log OOM, không coi ô trống là 0. Fit chọn 809,345s; thời gian resume chỉ là kiểm tra lại. |
| 5:20–6:00 | Báo cáo và giới hạn | Coverage 73,2495%, HitRate@5 14,4739%, mẫu số 131.209 người dùng. Train cũng dùng chọn mô hình: validation nội bộ. Nêu nhu cầu test độc lập, baseline và triển khai. |

## Nếu gặp sự cố

- [ ] Trang báo thiếu file: ghi nhận đường dẫn và pipeline được hiển thị; kiểm tra metrics/checkpoint trước khi chạy lại. Không tạo dữ liệu thay thế.
- [ ] Không có luật khớp: trình bày fallback phổ biến là hành vi thiết kế; dữ liệu fallback chỉ gồm top 20 đã lưu, có thể ít hơn năm kết quả.
- [ ] Dashboard chưa truy cập được: kiểm tra terminal/server và port 8501, khởi động lại lệnh Streamlit. Môi trường khởi động lại sẽ dừng server.
- [ ] Test hoặc đối soát FAIL: xem stage lỗi trong `prototype_summary.json` và log tương ứng; không demo bằng số liệu bịa hoặc bỏ qua lỗi.

## Sau demo

- [ ] Giữ JSON/CSV/PNG/HTML và log làm minh chứng; không xóa checkpoint/lần thử lỗi.
- [ ] Ghi rõ kiểm tra giao diện hiện dùng AppTest; kiểm tra trình duyệt thật trong buổi demo là bước bổ sung.
