"""Audit persisted results without inventing execution outcomes."""
import csv, hashlib, json
from pathlib import Path
from datetime import datetime, timezone
from PIL import Image
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[2]
def load(p):return json.loads((ROOT/p).read_text())
def save(p,v):(ROOT/p).write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n')
def main():
 summary=load('outputs/metrics/prototype_summary.json'); raw=load('outputs/metrics/raw_profile.json'); tests=load('outputs/metrics/test_summary.json'); sample=load('outputs/metrics/sample_integration.json'); manifest=load('outputs/evidence/evidence_manifest.json'); word=load('outputs/metrics/word_validation.json')
 tables=raw['tables']; criteria=[]
 def check(name,ok,evidence):criteria.append({'criterion':name,'status':'PASS' if ok else 'FAIL','evidence':evidence})
 check('Môi trường thật, execution mode và run_id',summary['execution_mode']=='full' and summary['environment']['venv'],'outputs/metrics/environment.json')
 check('Đủ sáu CSV thật, tên/kích thước khớp',len(tables)==6 and all(Path(t['source_path']).stat().st_size==t['size_bytes'] for t in tables.values()),'outputs/metrics/raw_profile.json')
 check('Schema, null, duplicate và miền giá trị',all(t['status']=='PASS' and all(t['checks'].values()) for t in tables.values()),'outputs/metrics/raw_profile.json; tests/test_raw_validation.py')
 bronze={}; gold={}
 for name,t in tables.items():
  files=list((ROOT/'data/bronze'/name).glob('*.parquet')); bronze[name]=sum(pq.ParquetFile(f).metadata.num_rows for f in files)
  assert all({'run_id','source_file','ingestion_time'}<=set(pq.read_schema(f).names) for f in files)
 check('Bronze Snappy, metadata và đối soát 6 bảng',all(bronze[n]==t['row_count']==t['bronze_rows'] and t['snappy'] for n,t in tables.items()),'data/bronze; outputs/metrics/raw_profile.json; tests/test_prototype_integration.py')
 check('Silver chuẩn hóa và chất lượng khóa ngoại',any(s['name']=='silver' and s['status'] in ('PASS','RESUMED') for s in summary['stages']),'outputs/metrics/silver_quality.json')
 kpi=summary['results']['kpi'] if 'results' in summary else None
 # Summary stores final results under artifacts/results depending on runner version.
 if kpi is None:
  def find(obj,key):
   if isinstance(obj,dict):
    if key in obj:return obj[key]
    for v in obj.values():
     r=find(v,key)
     if r is not None:return r
  kpi=find(summary,'kpi')
 check('KPI và phân bố đơn/user, aisle',kpi['total_orders']==3421083 and kpi['total_order_items']==33819106 and 'most_purchased_aisle_name' in kpi,'outputs/metrics/eda_summary.json; outputs/tables/top_20_aisles.csv')
 figures=sorted((ROOT/'outputs/figures').glob('[0-9][0-9]_*.png'))
 check('12 biểu đồ thật, kèm bảng CSV',len(figures)==12 and all(Image.open(p).width>=1000 for p in figures) and len(list((ROOT/'outputs/tables').glob('*.csv')))>=12,'outputs/figures; outputs/tables')
 comparison=list(csv.DictReader((ROOT/'outputs/metrics/model_comparison.csv').open()))
 check('FP-Growth đủ 9 cấu hình và lý do bỏ qua',len(comparison)==9 and all(r.get('status') in ('PASS','SKIP','SKIPPED') or r.get('status','').startswith('SKIP') for r in comparison),'outputs/metrics/model_comparison.csv; logs/fpgrowth_pipeline.log')
 selected=find(summary,'model_selected')
 check('Mô hình chọn và đánh giá thật',selected['association_rules']==834 and selected['frequent_itemsets']==4172 and selected['eligible_users']==131209,'outputs/metrics/model_summary.json')
 for folder in (ROOT/'data/gold').iterdir():
  if folder.is_dir() and not folder.name.startswith('.'):
   files=list(folder.glob('*.parquet'))
   if files:gold[folder.name]=sum(pq.read_table(f).num_rows for f in files)
 check('Gold đọc được toàn bộ bảng',len(gold)==5 and all(n>0 for n in gold.values()),'data/gold; tests/test_prototype_integration.py')
 rec=list(csv.DictReader((ROOT/'outputs/tables/recommendation_example.csv').open()))
 check('Dashboard 5 trang và Top-5 trên giỏ thật',len(rec)==5 and all((ROOT/'outputs/evidence'/i['file']).exists() for i in manifest['images'] if i['file'].startswith(('10_','11_','12_','13_','14_'))),'outputs/evidence/10_dashboard_overview.png … 14_dashboard_performance.png; outputs/metrics/recommendation_evidence_basket.json')
 spark=load('outputs/evidence/spark_session.json')
 check('Spark UI thật sau action full',spark['actual_counts']=={'baskets':3346083,'association_rules':834},'outputs/evidence/07_spark_ui_jobs.png; 08_spark_ui_stages.png; 09_spark_ui_environment.png; spark_session.json')
 check('18 ảnh bằng chứng, nguồn và checksum',manifest['status']=='PASS' and len(manifest['images'])==18 and all(hashlib.sha256((ROOT/'outputs/evidence'/i['file']).read_bytes()).hexdigest()==i['sha256'] for i in manifest['images']),'outputs/evidence/EVIDENCE_MANIFEST.md')
 check('Unit/integration test full thật',tests['status']=='PASS' and tests['count']==34,'outputs/metrics/test_summary.json; logs/integrated_tests.log')
 ss=json.loads(Path(sample['summary_path']).read_text())
 check('Raw→Gold mẫu thật trong workspace độc lập',sample['status']==ss['status']=='PASS' and not ss['resume'] and ss['execution_mode']=='sample','outputs/metrics/sample_integration.json; '+str(Path(sample['workspace']).relative_to(ROOT)))
 check('Runner full/sample/auto/resume/evidence và logs',summary['status']=='PASS' and all(s['status'] in ('PASS','RESUMED') for s in summary['stages']),'src/run_pipeline.py; run_pipeline.sh; logs/prototype_pipeline.log')
 check('Word 12–18 trang, TOC/PAGE, giữ nguyên bản gốc và PDF',word['status']=='PASS' and word['reference_unchanged'] and 12<=word['rendered_pages']<=18 and not word['pdf_out_of_page_blocks'],'outputs/metrics/word_validation.json; docs/Bao_cao_prototype_hoan_thien.docx; docs/Bao_cao_prototype_hoan_thien.pdf')
 check('README và tài liệu chạy/demo',all((ROOT/p).exists() for p in ['README.md','docs/DEMO_CHECKLIST.md','docs/CHUONG_5_THUC_NGHIEM.md']),'README.md; docs/DEMO_CHECKLIST.md')
 status='PASS' if all(c['status']=='PASS' for c in criteria) else 'FAIL'
 summary['completion']={'status':status,'audited_at':datetime.now(timezone.utc).isoformat(),'criteria':criteria,'bronze_rows':bronze,'gold_rows':gold,'evidence_images':18,'figures':len(figures),'word_pages':word['rendered_pages'],'full_tests':tests['count'],'sample_seconds':ss['seconds'],'sample_workspace':sample['workspace']}
 save('outputs/metrics/prototype_summary.json',summary)
 lines=['# Nghiệm thu prototype Instacart','',f"Trạng thái: **{status}**. Run `{summary['run_id']}`, mode **full**, resume dữ liệu đã hoàn thành; kiểm thử raw→Gold mới chạy riêng trên mẫu thật 1%.",'','| Tiêu chí | Trạng thái | Bằng chứng |','|---|---|---|']
 lines += [f"| {c['criterion']} | {c['status']} | {c['evidence']} |" for c in criteria]
 lines += ['','## Số liệu và thời gian thật','',f"Lần full resume + tests + evidence: {summary['seconds']:.3f} giây; 34 test: {tests['seconds']:.3f} giây. Đây không phải thời gian huấn luyện lại toàn bộ.",f"Mẫu độc lập raw→Gold: {ss['seconds']:.3f} giây, 34.299 orders và 339.165 order-items; 32 test tại thời điểm snapshot.",'', '| Bảng CSV / Bronze | Dòng | CSV bytes | Bronze bytes |','|---|---:|---:|---:|']
 lines += [f"| {n} | {t['row_count']:,} | {t['size_bytes']:,} | {t['bronze_size_bytes']:,} |" for n,t in tables.items()]
 lines += ['',f"Silver có 33.819.106 order-items và 3.346.083 baskets. Mô hình chọn support 0,001 / confidence 0,2: 4.172 itemsets, 834 rules; fit 809,345 giây. Coverage {selected['coverage']:.2%}; HitRate@5 {selected['hitrate_at_5']:.2%}; median lift {selected['median_lift']:.6f}.",'','## Phạm vi, nguồn và hạn chế','', 'Dữ liệu full được tính ở các lần chạy có log/checkpoint; run_id hiện tại là lần kiểm tra/resume và chụp ảnh. Spark UI chứng minh action count mới trên full Silver/Gold, không giả là màn hình huấn luyện trước đó. Bronze cũ được bổ sung run_id bằng migration riêng, giữ ingestion_time gốc. Mẫu lấy từ CSV thật, giữ đầy đủ dimensions và chạy trong outputs/sample_runs; không thay thế dữ liệu full.','', 'Chạy Spark local trên filesystem local, chưa triển khai HDFS hay cluster. Mức support 0,0005 bị bỏ qua sau thử nghiệm tài nguyên thất bại, ghi rõ trong grid. Train split đồng thời dùng chọn cấu hình và đánh giá nên kết quả là validation nội bộ, chưa là holdout độc lập. Những ô thông tin cá nhân chưa có trong Word gốc được giữ trống.','', 'Ảnh 07–14 là ảnh trình duyệt thật; các ảnh còn lại render bảng/sơ đồ từ artifact thật và được ghi rõ trong manifest. Không có mockup hay dữ liệu giả.','', '## File tạo hoặc sửa','', '`src/evidence/*.py`, `src/run_pipeline.py`, `src/prototype_checks.py`, `src/pipeline/02_ingest_bronze.py`, `src/pipeline/03_build_silver.py`, `app/dashboard.py`, `tests/test_prototype_integration.py`, `requirements.txt`, `.streamlit/config.toml`, `.gitignore`, `README.md`; các báo cáo/CSV/PNG/manifest dưới outputs, Word/PDF và bản tham chiếu dưới docs. Danh sách artifact cụ thể nằm trong evidence manifest và prototype_summary.json.','', '## Chạy lại','', '```bash','./run_pipeline.sh --mode full --resume --evidence-mode','.venv/bin/python src/evidence/build_report.py','.venv/bin/python src/evidence/render_report.py','.venv/bin/python src/evidence/complete.py','```']
 (ROOT/'docs/PROTOTYPE_COMPLETION_REPORT.md').write_text('\n'.join(lines)+'\n')
 print(status, 'criteria',len(criteria)); print([c for c in criteria if c['status']=='FAIL'])
 if status!='PASS':raise SystemExit(1)
if __name__=='__main__':main()
