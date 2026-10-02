"""Generate Chapter 5 from real experiment reports; no illustrative metrics."""
import json
import csv
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def read(name):return json.loads((ROOT/'outputs/metrics'/name).read_text())
def rows(name):return list(csv.DictReader((ROOT/'outputs/metrics'/name).open()))

def main():
    raw=read('raw_profile.json');silver=read('silver_profile.json');eda=read('eda_summary.json');model=read('model_summary.json');prototype=read('prototype_summary.json');env=read('experiment_environment.json')
    assert all(r['status']=='PASS' for r in [raw,silver,eda,model,prototype])
    k=eda['kpi'];c=model['selected'];test=next(s for s in prototype['stages'] if s['name']=='tests')
    lines=['# CHƯƠNG 5. THỰC NGHIỆM VÀ ĐÁNH GIÁ','',
           'Các số liệu trong chương được lấy từ báo cáo JSON/CSV của pipeline chạy trên dữ liệu Instacart thực tế. Hình được dẫn từ outputs/figures; không có số liệu minh họa hoặc mockup thay cho kết quả thực nghiệm. Ngày tổng hợp: 02/10/2026 (Asia/Bangkok).','',
           '## 5.1. Môi trường thực nghiệm','',
           f"Môi trường ghi nhận tại Task 6: Python {env['python']}, {env['platform']}, {env['logical_cpus']} CPU logic, {env['java'].splitlines()[0]}. Phiên bản package: "+', '.join(f'{n} {v}' for n,v in env['packages'].items())+'.', '',
           'Spark chung: local[*], driver 2g, AQE bật, shuffle partitions 32. FP-Growth dùng driver 3g, 128 phân vùng và task.cpus=4 để giảm số task đồng thời. Đây là prototype chạy local, không phải benchmark cluster. Snapshot môi trường: [experiment_environment.json](../outputs/metrics/experiment_environment.json).','',
           'Kiến trúc: CSV raw → Bronze Parquet Snappy → Silver chuẩn hóa/kiểm tra khóa ngoại/giỏ → KPI và EDA → FP-Growth học prior → đánh giá train và chọn mô hình → Gold → Streamlit. Dashboard chỉ đọc các bảng Gold nhỏ và metrics đã tính, không khởi động Spark.','',
           '## 5.2. Bộ dữ liệu và tiền xử lý','',
           'Sáu file được đặt trong data/raw; tên thực tế được giữ đúng theo cấu hình đọc, kể cả hai dấu gạch dưới trong tên order_products.','',
           '| Bảng | Tên file thực tế | Kích thước (byte) | CSV | Bronze đọc lại | Đối soát |',
           '|---|---|---:|---:|---:|---|']
    for name,v in raw['tables'].items():
        lines.append(f"| {name} | {Path(v['source_path']).name} | {v['size_bytes']:,} | {v['row_count']:,} | {v['bronze_rows']:,} | {v['reconciliation']} |")
    lines+=['', 'Spark khai báo schema tường minh, đọc CSV FAILFAST và kiểm tra header, số cột/dòng, null khóa, duplicate khóa, miền giá trị. Khóa của order_products là (order_id, product_id). days_since_prior_order được phép null ở đơn đầu. Bronze thêm ingestion_time/source_file và đối chiếu schema, Snappy, metadata, số dòng ở đường dẫn cuối.', '',
            'Silver gồm năm bảng. Kiểm tra khóa ngoại sản phẩm–aisle/department, item–order/product và eval_set trước khi nối; số vi phạm khóa ngoại đều bằng 0. Mã ngày 0–6 đã ẩn danh nên không gán tên thứ trong tuần.','',
            '| Silver | Số dòng | Dung lượng thư mục (byte) |', '|---|---:|---:|']
    for name,v in silver['tables'].items():lines.append(f"| {name} | {v['rows']:,} | {v['size_bytes']:,} |")
    lines+=['',f"Có {k['observed_baskets']:,} giỏ với chi tiết quan sát (prior + train); {k['orders_without_observed_items']:,} đơn test không có item. Không tạo giỏ rỗng giả cho test. Tổng phần tử các giỏ là {silver['basket_integrity']['total_items']:,}, khớp số item; giỏ vi phạm tính duy nhất/kiểu dữ liệu: {silver['basket_integrity']['invalid']}.",'',
            '## 5.3. Phân tích khám phá dữ liệu','', '| Chỉ số | Kết quả thực tế |','|---|---:|',
            f"| Người dùng | {k['total_users']:,} |",f"| Đơn hàng | {k['total_orders']:,} |",f"| Lượt mua quan sát | {k['total_order_items']:,} |",f"| Sản phẩm trong danh mục | {k['total_products']:,} |",f"| Kích thước giỏ trung bình | {k['avg_basket_size']:.6f} |",f"| Kích thước giỏ trung vị | {k['median_basket_size']:.0f} |",f"| Tỷ lệ reorder | {k['reorder_rate']:.6%} |",f"| Khoảng cách mua trung bình (ngày, bỏ null) | {k['avg_days_between_orders']:.6f} |",f"| Đơn/người dùng trung bình | {k['avg_orders_per_user']:.6f} |",'',
            'Reorder là tổng cờ reordered chia số item quan sát, không phải tỷ lệ người dùng mua lại. Khoảng cách mua phản ánh nguồn bị chặn ở 30 ngày. KPI đơn/người dùng dùng toàn bộ orders; KPI giỏ dùng giỏ quan sát. Trung vị là percentile chính xác.','',
            '![Đơn theo giờ](../outputs/figures/orders_by_hour.png)','',f"Đỉnh quan sát ở giờ {k['peak_hour']} với {k['peak_hour_orders']:,} đơn. Biểu đồ cho thấy phân bố theo giờ trong dataset; không suy luận múi giờ hay nguyên nhân từ dữ liệu ẩn danh.",'',
            '![Đơn theo mã ngày](../outputs/figures/orders_by_dow.png)','',f"Mã ngày {k['peak_order_dow']} có {k['peak_day_orders']:,} đơn, lớn nhất theo số đếm. Không gọi đây là Chủ nhật/Thứ Hai do không có ánh xạ.",'',
            '![Top sản phẩm](../outputs/figures/top_20_products.png)','',f"{k['most_purchased_product_name']} (product_id={k['most_purchased_product_id']}) đứng đầu với {k['most_purchased_product_count']:,} lượt mua. Đây là độ phổ biến toàn tập quan sát, không chứng minh sở thích của từng người.",'',
            '![Top department](../outputs/figures/top_15_departments.png)','',
            f"Department {rows('top_15_departments.csv')[0]['department']} đứng đầu với {int(rows('top_15_departments.csv')[0]['count']):,} lượt mua. Danh sách top 15 chỉ là phần nổi bật trong toàn bộ department.",'',
            '![Kích thước giỏ](../outputs/figures/basket_size_distribution.png)','',f"Trung bình {k['avg_basket_size']:.4f} lớn hơn trung vị {k['median_basket_size']:.0f}; các giỏ lớn kéo trung bình lên. Histogram có tổng số giỏ và tổng item khớp KPI, đã kiểm thử độc lập.",'',
            '![Reorder theo department](../outputs/figures/reorder_by_department.png)','',f"Department {rows('reorder_by_department.csv')[0]['department']} có reorder cao nhất trong bảng đã sắp xếp: {float(rows('reorder_by_department.csv')[0]['reorder_rate']):.4%}. Khi tổng hợp lại toàn tập phải lấy trọng số item_count, không trung bình đơn giản các tỷ lệ department.",'',
            '## 5.4. Xây dựng FP-Growth','',
            f"Chỉ học từ lịch sử prior: {model['preparation']['prior_baskets']:,} giỏ, loại {model['preparation']['excluded_single_item_baskets']:,} giỏ một sản phẩm, còn {c['training_baskets']:,} giỏ. items là ArrayType(IntegerType), không null, không trùng và gồm mã dương. Tên sản phẩm chỉ được nối lúc xuất cuối.",'',
            'Thử minSupport ∈ {0,0005; 0,001; 0,002} và minConfidence ∈ {0,20; 0,30; 0,40}. Mỗi support fit một lần ở confidence thấp nhất, rồi lọc ba mức confidence. Worker được cô lập; lỗi RAM/timeout hoặc vượt giới hạn số pattern/luật được ghi và dừng worker an toàn.','',
            'Support 0,0005 đã có OOM ở lần thử tài nguyên thấp hơn, và các lần thử sau bị gián đoạn khi môi trường khởi động lại. Resume bỏ qua theo log OOM cũ, không giả lập metrics cho cấu hình chưa hoàn tất. Bảng so sánh vẫn đủ chín tổ hợp, gồm sáu PASS và ba SKIPPED_OUT_OF_MEMORY.','',
            'Luật cuối yêu cầu lift > 1, đủ confidence, antecedent không rỗng, consequent một sản phẩm, hai vế không giao nhau và không có cặp luật trùng. Chọn mô hình với công thức đa tiêu chí công khai:', '', '`'+model['selection_method']+'`', '',
            '## 5.5. Kết quả và đánh giá','',
            '| Support | Confidence | Trạng thái | Itemsets | Luật | Lift TB | Lift trung vị | Coverage | HitRate@5 | Fit (s) |',
            '|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in model['comparisons']:
        if r['status']=='PASS':lines.append(f"| {r['min_support']} | {r['min_confidence']} | PASS | {r['frequent_itemsets']:,} | {r['useful_rules']:,} | {r['avg_lift']:.6f} | {r['median_lift']:.6f} | {r['coverage']:.4%} | {r['hitrate_at_5']:.4%} | {r['training_seconds']:.3f} |")
        else:lines.append(f"| {r['min_support']} | {r['min_confidence']} | {r['status']} | — | — | — | — | — | — | — |")
    lines+=['',f"Chọn minSupport={c['min_support']}, minConfidence={c['min_confidence']}: {c['frequent_itemsets']:,} itemset, {c['useful_rules']:,} luật; support TB {c['avg_support']:.8f}, confidence TB {c['avg_confidence']:.8f}, lift TB {c['avg_lift']:.8f}, lift trung vị {c['median_lift']:.8f}. Điểm lựa chọn {c['selection_score']:.8f}.",'',
            f"Đánh giá trên {c['eligible_users']:,} người dùng có đơn train: lấy giỏ prior cuối làm ngữ cảnh và toàn bộ item của đơn train làm đáp án. Kiểm tra train nằm sau ngữ cảnh và một đơn train/người. Khớp toàn bộ antecedent, loại sản phẩm đã có, xếp confidence/lift/support giảm dần, product_id tăng dần; tối đa năm sản phẩm duy nhất.",'',
            f"Coverage = {c['covered_users']:,}/{c['eligible_users']:,} = {c['coverage']:.6%}. HitRate@5 = {c['hit_users']:,}/{c['eligible_users']:,} = {c['hitrate_at_5']:.6%}. Cùng mẫu số gồm cả người không có gợi ý. HitRate có điều kiện trên người nhận gợi ý là {c['conditional_hitrate_at_5']:.6%}, chỉ là chỉ số bổ sung. Có {c['recommendation_count']:,} gợi ý top-5 được sinh trong đánh giá.",'',
            'Train không học luật nhưng đồng thời dùng để chọn cấu hình. Vì vậy đây là validation nội bộ, không phải ước lượng tổng quát hóa từ một test cuối độc lập. Loại sản phẩm đã có trong ngữ cảnh giới hạn nhiệm vụ ở sản phẩm mua kèm mới so với giỏ đó, không đánh giá đầy đủ nhu cầu reorder.','',
            '![Số luật](../outputs/figures/model_rule_counts.png)','',
            'Hạ support hoặc confidence giữ nhiều luật hơn. Ngưỡng confidence cao giảm độ bao phủ; số luật nhỏ có thể đạt lift lớn mà vẫn không đủ hữu dụng cho nhiều người. Các bar chỉ hiển thị cấu hình PASS; cấu hình bị bỏ qua không có giá trị 0 giả.','',
            '![Phân phối lift](../outputs/figures/model_lift_distribution.png)','',
            f"Phân phối dùng khoảng log₂ trên toàn bộ {c['useful_rules']:,} luật đã chọn; lift trung vị {c['median_lift']:.4f} thấp hơn lift TB {c['avg_lift']:.4f}, cho thấy nhóm luật lift lớn kéo trung bình lên. Tổng số luật ở các khoảng khớp Gold.",'',
            '![Top luật](../outputs/figures/model_top_20_rules.png)','',
            'Top 20 chỉ xét support ≥ 0,001, tránh trình bày lift lớn của pattern quá hiếm mà không nêu mức support. Tên sản phẩm được gắn từ dimension thực tế; bảng chi tiết nằm ở [top_rules_details.csv](../outputs/metrics/top_rules_details.csv). Lift lớn không đồng nghĩa quan hệ nhân quả.','',
            '![Confidence–lift](../outputs/figures/model_confidence_lift.png)','',
            'Biểu đồ offline biểu diễn mật độ luật theo khoảng confidence 0,05 và log₂(lift) 0,25; kích thước/màu phản ánh số luật trong khoảng. Không phải mỗi bubble là một luật riêng. Dashboard có scatter từng luật sau lọc.','',
            '## 5.6. Dashboard prototype','',
            'Streamlit Instacart Basket Intelligence gồm Tổng quan, Hành vi mua sắm, Luật kết hợp, Gợi ý sản phẩm, Hiệu năng hệ thống. Dùng layout wide, màu xanh đậm, Plotly, st.cache_data và giải thích tiếng Việt. Có lọc luật/tải CSV và chọn nhiều sản phẩm từ danh mục 49.688 mã. Gợi ý hiển thị confidence, luật nguồn, support/lift và loại sản phẩm đã chọn.','',
            'Không có luật khớp thì fallback xếp theo lượt mua thực tế trong top 20 phổ biến đã lưu, ghi rõ fallback và không tạo score/support/confidence/lift giả. Thiếu file sẽ chỉ file và lệnh pipeline, không tự chạy Spark. Bộ lọc chỉ tác động luật mô hình đã chọn, không huấn luyện lại.','',
            'Năm trang và các tương tác được kiểm tra bằng Streamlit AppTest với dữ liệu thật; hình EDA/mô hình ở chương là artifact thực nghiệm, không phải ảnh chụp giao diện dashboard. Không có mockup trong chương.','',
            '## 5.7. Đánh giá hiệu năng','',
            '| Thành phần | Thời gian thực tế (s) | Nguồn |','|---|---:|---|',
            f"| Task 2 validation + Bronze | {raw['total_seconds']:.3f} | raw_profile.json |",f"| Silver | {silver['total_seconds']:.3f} | silver_profile.json |",f"| KPI/EDA | {eda['total_seconds']:.3f} | eda_summary.json |",f"| Chuẩn bị giỏ FP-Growth | {model['preparation']['total_seconds']:.3f} | model_summary.json |"]
    for support in sorted({r['min_support'] for r in model['comparisons'] if r['status']=='PASS'},reverse=True):
        r=next(r for r in model['comparisons'] if r['status']=='PASS' and r['min_support']==support)
        lines.append(f"| Fit support={support} | {r['training_seconds']:.3f} | model_summary.json |")
    lines += [f"| Đánh giá/xuất Gold/biểu đồ | {model['evaluation_pipeline_seconds']:.3f} | model_summary.json |",f"| Task 6 resume + kiểm tra + test | {prototype['seconds']:.3f} | prototype_summary.json |",f"| Bộ test Task 6 ({test['tests']} test) | {test['test_seconds']:.3f} | integrated_tests.log |",'',
              '![Thời gian fit](../outputs/figures/model_training_times.png)','',
              'Các bar theo confidence của cùng support lặp cùng một thời gian fit; không cộng ba lần. Giảm support từ 0,002 xuống 0,001 tăng thời gian và số pattern trong hai lần fit thành công. Không có thời gian fit hoàn chỉnh cho 0,0005. Tổng end-to-end của toàn bộ các task không được khẳng định vì có nhiều lần OOM/gián đoạn; thời gian Task 6 là xác minh/resume và kiểm thử, không phải huấn luyện lại.','',
              '| Gold | Số dòng kiểm tra | Dung lượng thư mục (byte) |','|---|---:|---:|']
    for name,v in prototype['gold'].items():lines.append(f"| {name} | {v['rows']:,} | {v['bytes']:,} |")
    lines +=['',f"Tổng dung lượng năm thư mục Gold: {sum(v['bytes'] for v in prototype['gold'].values()):,} byte. Kiểm tra decode đầy đủ bảng Gold, metadata/schema/compression và mẫu đọc từ từng fragment Bronze/Silver/checkpoint. Đây không phải full scan DQ mới trên mọi giao dịch; DQ toàn tập dùng kết quả Spark ở các stage trước và test bổ sung.",'',
              f"Task 6 chạy {test['tests']} test đạt, gồm test raw/schema/null/duplicate trên dữ liệu thật, kiểm tra Silver/EDA, FP-Growth và evaluation, dashboard, integration trên 64 giỏ thật, cơ chế resume và truyền exception. Test cố ý gây lỗi để kiểm tra fail-fast không phải dữ liệu thực nghiệm mới; log lỗi dự kiến thuộc test đã PASS.",'',
              'Runner khóa để tránh hai phiên đồng thời, ghi timestamp bắt đầu/kết thúc, số dòng theo bảng và trạng thái mỗi stage; ghi JSON atomic, dừng chuỗi khi lỗi và truyền exception. Resume kiểm tra PASS và artifact trước khi bỏ qua; dấu raw/config đổi thì không tiếp tục mù với đầu ra cũ. --no-resume yêu cầu workspace sạch, không xóa hay ghi đè dữ liệu hiện có. Pipeline fresh toàn tập không chạy lại trong Task 6 vì dữ liệu đã hoàn tất; đường resume thực tế đã được kiểm tra.','',
              '## 5.8. Hạn chế và hướng phát triển','',
              '- Dữ liệu ẩn danh, thiếu lịch/giá/tồn kho; thời gian giữa đơn chặn 30 ngày. Không suy diễn nguyên nhân từ tương quan.',
              '- Support 0,0005 chưa có kết quả hoàn chỉnh do tài nguyên và gián đoạn; không kết luận mô hình ở ngưỡng này kém chất lượng.',
              '- Train dùng để chọn mô hình: cần tách validation/test theo thời gian hoặc người dùng và so sánh baseline popularity, thêm Precision/Recall/NDCG@5 và đánh giá reorder.',
              '- Confidence chưa được hiệu chỉnh thành xác suất cá nhân. Mô hình tập trung sản phẩm mua kèm so với giỏ; chưa có personalized ranking, cold-start chuyên biệt hay bộ lọc tồn kho.',
              '- Fallback giới hạn top 20; mở rộng bảng popularity theo sản phẩm/department và đánh giá hiệu quả fallback riêng.',
              '- Local prototype, chưa benchmark phân tán, chưa có CI/deploy/auth. Cần profiling bộ nhớ, resource budget và kiểm thử fresh pipeline trong môi trường đủ RAM.',
              '- Resume dùng fingerprint tên/kích thước/mtime raw và nội dung config, không hash toàn bộ CSV; cần content hash/version dataset và lineage chặt hơn cho production.',
              '- Kiểm tra UI bằng AppTest; chưa kiểm tra trình duyệt thủ công và khả năng hiển thị trên nhiều thiết bị.','',
              'Nguồn kiểm chứng: [prototype_summary.json](../outputs/metrics/prototype_summary.json), [raw_profile.json](../outputs/metrics/raw_profile.json), [silver_profile.json](../outputs/metrics/silver_profile.json), [eda_summary.json](../outputs/metrics/eda_summary.json), [model_summary.json](../outputs/metrics/model_summary.json), [model_comparison.csv](../outputs/metrics/model_comparison.csv), [integrated_tests.log](../logs/integrated_tests.log).']
    (ROOT/'docs/CHUONG_5_THUC_NGHIEM.md').write_text('\n'.join(lines)+'\n')
    print('Chapter 5 generated from PASS reports.')

if __name__=='__main__':main()
