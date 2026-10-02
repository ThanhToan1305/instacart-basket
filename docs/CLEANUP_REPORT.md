# Dọn file local trước lần push đầu tiên

Ngày: 2026-10-03. Người dùng xác nhận chưa push GitHub, tất cả chạy local. Đã rà cấu trúc, source, JSON runtime, dependency và file dự kiến public; không khởi tạo Git root/commit/push.

## Kết quả dọn

Đã xóa **109 file**, tổng **153,503,549 bytes** (~146.39 MiB):

- Bytecode/cache Python dưới src/app/tests.
- Gói .deb LibreOffice đã giải nén: giữ bản LibreOffice runtime đã cài local; chạy helper setup lần sau sẽ tải lại các gói nếu cần.
- Debug trace tạm của quá trình cài LibreOffice.
- Stop marker của các worker chụp evidence đã kết thúc.

Danh sách từng file/byte/lý do: [CLEANUP_REMOVED_FILES.csv](CLEANUP_REMOVED_FILES.csv). Không tìm thấy source Python lỗi cú pháp hoặc JSON runtime lỗi parse trong phạm vi đã đọc, nên không xóa source theo phỏng đoán.

## Những thứ được giữ

Giữ toàn bộ raw, Bronze/Silver/Gold, checkpoint/backup và sample run thật; chúng có thể được checker/test/resume dùng. Metadata size/mtime của **2002 file data** trước/sau dọn giống nhau. Giữ venv đang chạy, công cụ LibreOffice runtime, 12 biểu đồ đánh số, 18 evidence PNG, trang PDF raster, metrics và log huấn luyện/test. Log lỗi worker là provenance cho cấu hình bị bỏ qua, không phải file source hỏng. Giữ cả script smoke Spark lịch sử và wrapper Silver vì docs/code đang tham chiếu.

Các file local này bị ignore khi chia sẻ; không cần xóa dữ liệu hoạt động để chuẩn bị GitHub. Bổ sung ignore ba CSV model_comparison/model_attempt có worker_log chứa đường dẫn máy; vẫn giữ bản gốc local.

## .git và thư mục môi trường

`.git`, `.aws`, `.agents`, `.codex` hiển thị rỗng trong sandbox và thuộc vùng read-only/mount do môi trường. Xóa `.git` bằng rmdir trong sandbox báo **Device or resource busy**; thử ngoài sandbox báo **No such file or directory**. Không có metadata/history Git để xóa được xác minh; không cố gỡ mount hay thay quyền bảo vệ. Các thư mục account/Agent đã ignore. Không khẳng định những thư mục này do Agent tạo.

Trong terminal local thật, chủ project tự kiểm tra và khởi tạo repository mới nếu chưa có. Không dùng rm -rf .git: có thể mất lịch sử khi repository thật được tạo sau này.

## File thiếu trước khi dọn

Hiện không có src/doc, docs/reference, các báo cáo task_01…task_06, docs/CHUONG_5_THUC_NGHIEM.md và Word/PDF hoàn thiện. Chúng **đã thiếu ở lần khảo sát đầu tiên của cleanup**, không thuộc danh sách đã xóa. Đã sửa liên kết thiếu trong README/checklist và ghi rõ giới hạn tái tạo Word trong setup. Không tạo tài liệu giả để lấp chỗ thiếu. Cần phục hồi Word tham chiếu nếu muốn sử dụng build_report/render_report. Nghiệm thu cũ trong PROTOTYPE_COMPLETION_REPORT.md là lịch sử, không phải xác minh mới rằng các file Word/PDF còn tồn tại.

## Kiểm tra

- 41 Python files parse AST; 2 runner safety tests và 6 Silver/EDA tests **PASS**.
- Bash syntax PASS. Không chạy full pipeline/training.
- Scan mẫu private key/provider tokens ở các file text public không có match; không in giá trị bí mật.
- File public không có đường dẫn máy hoặc file ≥10 MiB tại thời điểm scan. Source/data/logs không bị đưa vào Git tự động.
- Số file dự kiến public: **153**, theo .gitignore và phần scaffolding hiện có; xem [PUBLISH_FILES.txt](PUBLISH_FILES.txt). Đây là inventory dự kiến, không phải Git index thật.
- Link Markdown local được kiểm tra lại sau khi viết report. Không có chứng nhận clean install trên máy khác.

## Chuẩn bị push

Trong terminal local, từ root, khởi tạo Git nếu xác nhận chưa có repo:

```bash
git init -b main
git status --short --untracked-files=all
```

So sánh danh sách với PUBLISH_FILES.txt; stage chọn lọc source/docs/ảnh đã review rồi kiểm tra trước commit:

```bash
git add README.md AGENTS.md .gitignore requirements.txt requirements-lock.txt run_pipeline.sh
git add src app config tests docs .streamlit/config.toml
git add data/raw/.gitkeep data/raw/README.md
git add outputs/figures outputs/evidence outputs/tables outputs/metrics
git diff --cached --check
git diff --cached --stat
git diff --cached
git ls-files data .venv logs
```

Danh sách cuối chỉ được có scaffolding data được phép; không có CSV raw/Parquet/venv/log/secrets. Xem [TEAM_WORKFLOW.md](TEAM_WORKFLOW.md) cho commit/remote/push. Các command chưa được Agent thực thi trên repository thật.
