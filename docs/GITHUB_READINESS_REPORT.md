# Báo cáo chuẩn bị source cho GitHub

Cập nhật sau cleanup 2026-10-03: người dùng xác nhận chưa push, chỉ chạy local. Một số tài liệu/Word/task gốc không còn trước lần cleanup; các vị trí liệt kê bên dưới là snapshot audit trước đó, không phải inventory hiện tại. Xem [CLEANUP_REPORT.md](CLEANUP_REPORT.md) và [PUBLISH_FILES.txt](PUBLISH_FILES.txt) cho trạng thái mới nhất. .git rỗng hiện bị môi trường mount bảo vệ; ngoài sandbox không thấy thư mục. Không xác nhận có lịch sử Git.

Ngày rà soát: 2026-10-03. Phạm vi: tài liệu, Git ignore và snapshot dependencies; không đổi thuật toán/nghiệp vụ, schema hoặc kết quả prototype; không xóa/rename/move dữ liệu; không chạy full pipeline, không commit/push.

## Cấu trúc đã khảo sát

`src/common/`, `src/pipeline/`, `src/evidence/`, runner/checkers/chapter writer, `app/`, `tests/`, `config/`, `.streamlit/`, requirements, shell, docs và các lớp data/outputs đã được đọc/kiểm tra. 41 file Python parse AST thành công. `notebooks/` hiện không có notebook. Source dùng DataFrame/Spark SQL expressions (percentile), joins và window; không có file SQL riêng. Entry points thật: `run_pipeline.sh`, `src/run_pipeline.py`, `app/dashboard.py`.

Cấu trúc/data flow và các lệnh cụ thể nằm trong [README](../README.md), [setup](SETUP_GUIDE.md) và [AGENTS](../AGENTS.md).

## File tạo/cập nhật

| Loại | File | Mục đích |
|---|---|---|
| Tạo | docs/SETUP_GUIDE.md | WSL/Java/Python, setup, từng stage, mode, dashboard/test/troubleshooting |
| Tạo | docs/TEAM_WORKFLOW.md | Nhánh, PR, conflict, dependency và staged-file review |
| Tạo | AGENTS.md | Bản đồ source, quy tắc và hạn chế cụ thể |
| Tạo | requirements-lock.txt | pip freeze từ venv, đối chiếu chính xác dependencies hiện có |
| Tạo | data/raw/README.md | Sáu tên CSV và cách chia sẻ riêng |
| Tạo | docs/GITHUB_READINESS_REPORT.md | Bằng chứng rà, tồn tại và việc chủ repo cần làm |
| Cập nhật | README.md | Thêm onboarding/navigation, giữ thông tin hữu ích của từng task |
| Cập nhật | .gitignore | Dataset/cache/secrets ignored; giữ PNG/HTML/evidence và CSV aggregate |

Không tạo `.env.example`: source không đọc .env. Ngoại lệ ignore vẫn cho phép `.env.example` an toàn nếu sau này có yêu cầu thật. Không đổi requirements.txt, không thêm/nâng cấp packages. Snapshot lock gồm 125 package, installed version khớp 125/125 requirements, pip check không phát hiện dependency hỏng; snapshot không có hash và không phải requirements tối thiểu.

## Kết quả kiểm tra thực tế

| Kiểm tra | Kết quả | Giới hạn |
|---|---|---|
| 2 runner safety tests | PASS | Intentional failure test có traceback dự kiến, assertion vẫn PASS |
| 6 Silver/EDA artifact tests | PASS | Đọc kết quả thật, không huấn luyện/khởi động Spark |
| Bash syntax + runner --help | PASS | Xác minh option full/sample/auto/resume/evidence |
| Python AST | PASS (41 file) | Kiểm tra cú pháp, không thay full suite |
| pip check và requirements version | PASS | Chưa cài sạch từ đầu trên máy khác |
| Git ignore rules | PASS (29/29 cases) | Git check-ignore dùng metadata tạm ngoài project, không thay .git hiện tại |
| Markdown links / command file paths | PASS (89 local links, 27 script references) | Link/file có thật; kiểm tra README, AGENTS, toàn bộ Markdown docs/src-doc và README raw; không kiểm chứng clone URL placeholder |
| Scan secrets rõ ràng ở public text | Không có mẫu token/key/password thật được phát hiện | Scan mẫu không chứng minh không còn bí mật; binary cần review thủ công |
| Đường dẫn máy trong tài liệu mới/README | Không có | Báo cáo/task lịch sử còn đường dẫn, xem bên dưới |
| File public >10 MiB sau ignore | Không có | Dataset/venv/tools vẫn trên máy, không bị xóa |
| git status / ls-files / remote | CHƯA XÁC MINH | Thư mục .git hiện rỗng, không phải repository hợp lệ |

Dataset/Parquet, venv, logs, Spark caches/checkpoints, secrets và account configs, sample workspaces, tools, JSON runtime không thuộc phạm vi public mặc định. Giữ source, docs, PNG/HTML biểu đồ, PNG evidence/manifest, CSV aggregate. Tổng kích thước trước ignore xấp xỉ: raw 681 MiB, Bronze 259 MiB (gồm backup), Silver 1,1 GiB, Gold 207 MiB (gồm checkpoint), venv 1,5 GiB, tools 512 MiB, sample 83 MiB. Đây là disk usage lịch sử khảo sát, không phải chỉ các bảng publish.

## Đường dẫn tuyệt đối và tính di động

Đã tìm các dạng đường dẫn Unix home/mount và ổ Windows C/D. Không thấy đường dẫn máy hard-code trong Python, app, tests hoặc YAML. Source tính root bằng pathlib. Các đường dẫn dưới đây có trong task gốc/báo cáo lịch sử/CSV aggregate, chỉ ghi vị trí; không sửa bằng chứng ngoài phạm vi:

| File | Dòng | Đề xuất |
|---|---:|---|
| outputs/tables/model_comparison.csv | 2 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 3 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 4 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 5 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 6 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 7 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 8 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 9 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/tables/model_comparison.csv | 10 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_03_report.md | 56 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| src/doc/01_Kiem_tra_va_chuan_hoa_project.md | 5 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 37 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 38 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 39 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 40 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 41 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| docs/task_02_report.md | 42 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 2 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 3 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 4 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 88 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 172 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 256 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 257 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 258 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_attempt_01.csv | 259 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 2 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 3 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 4 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 5 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 6 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 7 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 8 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 9 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| outputs/metrics/model_comparison.csv | 10 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |
| src/doc/02_Kiem_tra_dataset_va_tao_Bronze.md | 3 | Dùng relative path khi xuất bản bản tài liệu/metadata công khai; giữ bản bằng chứng gốc riêng |

Metrics runtime có thêm nhiều absolute paths trong raw/silver/model/prototype reports và environment; được ignore. CSV model comparison còn worker_log tuyệt đối và một số bảng/ảnh mô tả môi trường chạy; phải rà khi chủ repo quyết định công khai, không suy ra đã sanitize. Word/PDF có thể có thông tin cover/metadata của tác giả; chưa chứng nhận không có thông tin cá nhân trong binary. Không tự thay nội dung/bằng chứng cũ.

Vấn đề portability: metrics ghi run_dir/per_user_path tuyệt đối, runner fingerprint dựa size/mtime raw và YAML; không đảm bảo resume artifact copy từ máy khác. Giải pháp đề xuất cho task riêng: xuất metadata tương đối + resolve tại root, kiểm tra migration/checker/tests trước sửa. `complete.py` nghiệm thu full snapshot cụ thể; renderer chỉ dùng LibreOffice user-local. Runner dùng fcntl/Linux: Windows cần WSL2.

## Trạng thái GitHub readiness

**Tài liệu và quy tắc ignore đã hoàn tất; chưa thể xác nhận repository sẵn sàng push hoàn toàn.** `.git` không hợp lệ nên không biết file nào đã track, remote nào đang dùng hay lịch sử có bí mật. Ignore không ngừng track file cũ. Chưa thử clean install và clone thật. Không tự khởi tạo Git root trong lần làm này.

Các warning trước push: review absolute paths lịch sử/CSV worker_log và binary/ảnh metadata cá nhân; kiểm tra index không có dataset/venv/log/secrets; xác nhận quyền chia sẻ dataset ngoài repo; URL/branch là cấu hình chủ repo; validation mô hình vẫn nội bộ, không biến thành kết quả test độc lập trong README.

## Lệnh chủ repository tự chạy

Nếu đã có repository ở vị trí khác, mở đúng clone hợp lệ; nếu đây là thư mục mới, cần chủ repo xác nhận khởi tạo Git. Không xóa thư mục .git rỗng để làm theo hướng dẫn. Chỉ thực hiện lệnh dưới đây khi quyết định tạo repo mới tại đây:

```bash
git init -b main
git status --short
git ls-files
git remote -v
```

Chỉ thêm remote nếu chưa có origin; `<REPOSITORY_URL>` cần thay bằng URL thật, không nhúng token:

```bash
git remote add origin <REPOSITORY_URL>
```

Stage tài liệu/rules chọn lọc; kiểm tra dataset không bị track rồi review staged diff:

```bash
git add README.md AGENTS.md .gitignore requirements.txt requirements-lock.txt
git add docs/SETUP_GUIDE.md docs/TEAM_WORKFLOW.md docs/GITHUB_READINESS_REPORT.md data/raw/README.md
git diff --cached --check
git diff --cached --stat
git diff --cached
git ls-files data .venv logs
git status --short
```

Nếu repository mới, thêm source/config/tests và các artifact đã review bằng danh sách đường dẫn chọn lọc; chưa có commit source từ Agent. Không dùng add toàn bộ để tránh kéo lịch sử nhạy cảm vào index. Chỉ khi chủ repo đã rà đủ staged contents và branch/remote:

```bash
git commit -m "docs: chuan bi huong dan va quy tac GitHub"
git push -u origin main
```

Với repository đã tồn tại, dùng feature branch và PR theo [workflow nhóm](TEAM_WORKFLOW.md), không push trực tiếp main. Nếu dataset/venv đã track, chỉ đề xuất git rm --cached như workflow; Agent không thực hiện và không xóa file.
