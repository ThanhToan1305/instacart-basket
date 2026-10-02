# Dữ liệu nguồn Instacart

Đặt sáu CSV thật vào thư mục này từ kênh chia sẻ dataset riêng của nhóm:

- `aisles.csv`
- `departments.csv`
- `products.csv`
- `orders.csv`
- `order_products__prior.csv`
- `order_products__train.csv`

Hai file cuối có hai dấu gạch dưới. Schema/header/keys kiểm tra tại `src/pipeline/raw_validation.py` tính từ gốc project. Không chỉnh raw để né validation, không tạo dữ liệu giả, không commit CSV vào GitHub. Chỉ `.gitkeep` và README này được giữ bởi quy tắc ignore. Xem [hướng dẫn setup](../../docs/SETUP_GUIDE.md).
