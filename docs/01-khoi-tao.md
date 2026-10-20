# Khởi tạo trên Windows

## 1. Công cụ

Cài Git và VS Code nếu máy chưa có. Cài Docker Desktop, chọn backend WSL 2 và
Linux containers. Khởi động Docker Desktop trước khi chạy kiểm tra.
Cài Python 3 nếu chưa có; công cụ check_data chỉ dùng thư viện chuẩn.

Nguồn chính thức:
- https://docs.docker.com/desktop/setup/install/windows-install/
- https://git-scm.com/downloads/win
- https://code.visualstudio.com/
- https://www.python.org/downloads/windows/
- https://www.kaggle.com/competitions/instacart-market-basket-analysis/data

## 2. Xác minh môi trường

Chạy từng dòng trong PowerShell thường:

```powershell
git --version
docker version
docker compose version
python --version
Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory, NumberOfLogicalProcessors
Get-PSDrive -PSProvider FileSystem
```

Nếu python không được tìm thấy, thử `py --version`; khi đó dùng `py` thay `python`.
Docker phải trả thông tin cả Client và Server. Nếu chỉ có Client, kiểm tra Docker
Desktop và Linux containers trước khi triển khai HDFS.
Không cần cài riêng Spark, Hadoop hoặc Java lên Windows trong hướng triển khai này.

## 3. Khởi tạo Git

Tại thư mục gốc project mới:

```powershell
git init
git add .
git commit -m "Initialize clean Instacart project"
```

Nếu Git yêu cầu danh tính, đặt tên/email của bạn cho repository này bằng
`git config user.name` và `git config user.email`, rồi chạy lại commit.
Không xóa hoặc ghi đè project cũ. Chưa cần kết nối GitHub để bắt đầu.

## 4. Dữ liệu

Tải bộ Instacart chính thức từ Kaggle, chấp nhận điều kiện tải nếu được yêu cầu.
Giải nén tới data/raw, sao cho sáu CSV nằm trực tiếp trong thư mục đó:

- aisles.csv
- departments.csv
- products.csv
- orders.csv
- order_products__prior.csv
- order_products__train.csv

Chạy:

```powershell
python scripts/check_data.py
```

PASS ở bước này chỉ xác nhận tên file, dung lượng khác 0 và header có đủ cột.
Nó chưa xác nhận số dòng, khóa ngoại, giá trị, chất lượng hay toàn vẹn dataset.

## 5. Đầu ra cần giữ

Giữ kết quả kiểm tra môi trường và check_data để chọn tài nguyên container ở bước
sau. Chưa chạy full dataset hoặc chọn phiên bản image trước khi kiểm tra máy.
