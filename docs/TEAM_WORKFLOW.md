# Quy trình làm việc nhóm

Tài liệu mô tả lệnh để thành viên tự thực hiện; Agent chuẩn bị tài liệu không commit hoặc push. URL remote và nhánh main thật chưa được xác minh trong workspace hiện tại. Xem [setup](SETUP_GUIDE.md) để tạo venv/dataset riêng.

## Clone và nhánh cá nhân

Thay placeholder bằng URL nhóm cung cấp:

```bash
git clone <REPOSITORY_URL> instacart-prototype
cd instacart-prototype
git status --short
git remote -v
```

Nếu chưa có repository trên GitHub, chủ dự án tạo repo trước; không đưa `.venv`, raw hay checkpoint vào initial commit. Không làm trực tiếp trên main. Khi working tree sạch, cập nhật main rồi tạo nhánh:

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/ten-chuc-nang
```

Trước buổi làm tiếp theo, xem trạng thái và pull nhánh của mình. Nếu có sửa chưa lưu, commit chọn lọc hoặc stash chọn file đã kiểm tra trước khi pull; không stash toàn bộ kèm bí mật/dataset. Nếu fast-forward không được, đọc lịch sử, không force reset:

```bash
git status --short
git pull --ff-only
```

## Sửa, kiểm tra, commit và Pull Request

Mỗi thay đổi nên nhỏ, có mục tiêu rõ. Chạy test phù hợp theo AGENTS.md; không chạy full training chỉ vì sửa docs. Chọn từng file khi stage:

```bash
git diff --check
git diff -- README.md docs/SETUP_GUIDE.md
git add README.md docs/SETUP_GUIDE.md
git diff --cached --stat
git diff --cached
git status --short
git commit -m "docs: huong dan setup prototype"
git push -u origin feature/ten-chuc-nang
```

Các file ở ví dụ là file thật; thay danh sách bằng đúng phạm vi task. Tránh `git add .` trước khi rà bỏ qua/bí mật. Commit message dùng tiền tố `docs:`, `feat:`, `fix:`, `test:` hoặc `chore:` + hành động cụ thể. Không đưa token vào command/commit message.

Trên GitHub, mở Pull Request từ feature branch sang main; ghi vấn đề, thay đổi, test đã chạy, ảnh minh chứng nếu thay UI và hạn chế. Người khác review trước merge; không push force lên main. Cập nhật nhánh từ main khi cần, giữ thay đổi của đồng đội.

## Conflict an toàn

Bắt đầu khi working tree sạch. Lấy main và merge vào nhánh cá nhân:

```bash
git fetch origin
git merge origin/main
git status --short
```

Mở từng file conflict, hiểu hai phía, giữ nghiệp vụ và dữ liệu đối soát; không chọn ours/theirs hàng loạt. Xóa marker conflict sau khi giải quyết, chạy kiểm tra, stage đúng file và tạo merge commit. Nếu chưa hiểu conflict, hỏi tác giả và dùng lệnh sau để quay lại trước merge:

```bash
git merge --abort
```

Không dùng reset --hard, clean hoặc xóa dataset để giải conflict. File Word/PDF là binary: phối hợp chọn bản đúng và tái tạo từ metrics đã kiểm chứng, không ghép nội dung tùy tiện.

## Những thứ không commit

- `.venv/`, `venv/`, cache Python/Jupyter, Spark warehouse/metastore/checkpoints.
- Sáu CSV raw và toàn bộ Parquet Bronze/Silver/Gold, cả staging/backup/model checkpoint ẩn.
- `.env` và biến thể; `.aws/`, `.codex/`, `.agents/`, Streamlit secrets, private key/certificate.
- `logs/`, log worker, LibreOffice profile/cache/tool binary và sample workspace.
- Metrics JSON runtime chứa đường dẫn/môi trường máy; CSV raw_profile có đường dẫn máy.

Giữ source, YAML an toàn, `.gitkeep`, README dataset, báo cáo đã rà thông tin cá nhân, PNG/HTML biểu đồ và PNG/manifest bằng chứng. `.gitignore` không bảo vệ file đã được track: xem index trước mỗi push.

Dataset chia sẻ qua kênh riêng có quyền truy cập phù hợp; dùng bộ Instacart thật và cùng tên/header. Không gửi dataset bằng commit/LFS trong task này. Nếu cần chỉ chia sẻ dashboard không rebuild, trao đổi riêng bundle Gold + metrics tương ứng và provenance; không copy mẫu vào kết quả full. Notebook hiện chưa có file, khi thêm phải xóa output chứa bí mật/đường dẫn cá nhân trước commit.

## Thêm dependency

`requirements.txt` là file chính; `requirements-lock.txt` snapshot từ venv đã kiểm chứng. Chỉ thêm thư viện source thật sự dùng, ghim version đã thử, không pip freeze từ môi trường toàn cục. Kiểm tra:

```bash
python -m pip check
python -m pip freeze
```

Sau khi đã review môi trường dự án và phạm vi dependencies, có thể cập nhật snapshot:

```bash
python -m pip freeze > requirements-lock.txt
```

Review diff cả hai file, chạy test liên quan và ghi Python/Java khi khác baseline. Nếu requirements đang chứa dependencies gián tiếp/Jupyter, việc tách requirements tối thiểu là task riêng, không tự dọn trong PR docs.

## Checklist gửi code và merge

Trước gửi: đúng nhánh; không sửa raw/metrics thật để làm test pass; không còn conflict; test phù hợp pass; link Markdown và lệnh đúng file; docs mô tả đúng full/sample; dependencies được ghi nếu có; staged diff không chứa dataset, venv, log, bí mật hoặc đường dẫn cá nhân.

Trước merge: reviewer hiểu thay đổi; schema Bronze/Silver/Gold không bị đổi ngoài phạm vi; gợi ý vẫn khớp đủ antecedent và loại sản phẩm trong giỏ; validation nội bộ không bị mô tả là test độc lập; bằng chứng/source nhất quán; CI/test nếu có pass. Không khẳng định CI đã tồn tại: hiện chưa có workflow CI trong source.

## Nếu file xấu đã được track

Chỉ đề xuất, chủ repository tự quyết định và kiểm tra; lệnh dưới đây bỏ khỏi index, giữ file trên máy:

```bash
git ls-files data .venv logs
git rm -r --cached -- .venv data/raw data/bronze data/silver data/gold logs
```

Chỉ chọn path thực sự đã track; giữ `.gitkeep`/README cần thiết và stage lại chúng theo `.gitignore`. Nếu bí mật từng được commit, bỏ track không xóa khỏi history: thu hồi bí mật và trao đổi cách dọn lịch sử với nhóm trước push.
