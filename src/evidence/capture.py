"""Capture actual browser pages; render provenance tables, never fake UIs."""
import sys,os,json,time,subprocess,argparse,re,hashlib
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
import pyarrow.parquet as pq
from PIL import Image,ImageStat
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright
from app.recommender import recommend
ROOT=Path(__file__).resolve().parents[2];FOLDER=ROOT/'outputs/evidence'

def timestamp():return datetime.now(ZoneInfo('Asia/Bangkok')).isoformat(timespec='seconds')
def read(name):return json.loads((ROOT/'outputs/metrics'/name).read_text())


def sheet(filename,title,content,source,run_id,records):
    mode=read("prototype_summary.json").get("execution_mode","full")
    if isinstance(content,str):
        lines=content.splitlines();height=max(4,min(18,len(lines)*.23+1.8))
        fig,ax=plt.subplots(figsize=(15,height));ax.axis('off');ax.text(.01,.98,content,va='top',fontfamily='DejaVu Sans Mono',fontsize=10)
    else:
        data=content.copy();fig,ax=plt.subplots(figsize=(17,max(4,min(14,len(data)*.38+2))));ax.axis('off')
        table=ax.table(cellText=data.fillna('—').astype(str).values,colLabels=data.columns,loc='center',cellLoc='left');table.auto_set_font_size(False);table.set_fontsize(9);table.scale(1,1.8)
        for (row,col),cell in table.get_celld().items():
            if row==0:cell.set_facecolor('#145C43');cell.get_text().set_color('white')
    fig.suptitle(title,color='#145C43',fontsize=16)
    created=timestamp();fig.text(.01,.035,f'run_id={run_id} | {mode} | {created}\nNguồn: {source}',fontsize=9)
    fig.tight_layout(rect=(0,.1,1,.94));fig.savefig(FOLDER/filename,dpi=180);plt.close(fig)
    records.append({'file':filename,'description':title,'source':source,'kind':'rendered real artifact/table; not a UI screenshot','run_id':run_id,'execution_mode':mode,'created_at':created})


def capture(run_id):
    mode=read("prototype_summary.json").get("execution_mode","full")
    port=8501 if mode=="full" else 8502
    dashboard_url=f"http://127.0.0.1:{port}"
    FOLDER.mkdir(exist_ok=True)
    (FOLDER/'evidence_manifest.json').write_text(json.dumps({'run_id':run_id,'status':'RUNNING','images':[]},indent=2)+'\n')
    context={'run_id':run_id,'execution_mode':mode,'created_at':timestamp()}
    (ROOT/'outputs/metrics/evidence_context.json').write_text(json.dumps(context,indent=2)+'\n')
    records=[];tree=[]
    for folder in ['app','config','data/raw','data/bronze','data/silver','data/gold','docs','logs','notebooks','outputs/figures','outputs/metrics','outputs/evidence','outputs/tables','src/common','src/pipeline','src/evidence','tests']:
        path=ROOT/folder;entries=[p.name+('/' if p.is_dir() else '') for p in sorted(path.iterdir()) if not p.name.startswith('.') and not p.name.startswith('__')]
        tree.extend([folder+'/', '  '+', '.join(entries)[:135]])
    sheet('01_project_structure.png','Cấu trúc project thực tế','\n'.join(tree),'filesystem snapshot; capture.py',run_id,records)
    sheet('02_pipeline_architecture.png','Kiến trúc pipeline triển khai thực tế',
          '6 CSV raw\n   ↓ validation: schema / null / duplicate / domains\nBronze: Parquet Snappy + ingestion_time / source_file / run_id\n   ↓ normalization + FK validation\nSilver: orders / products / items / enriched / baskets\n   ├─ KPI / EDA aggregates\n   └─ prior baskets ≥ 2 → FP-Growth → train validation\n                ↓ selection / lift > 1 / single consequent\nGold: KPI / frequent itemsets / rules / lookup / catalog\n   ↓ cached aggregate reads (NO Spark startup)\nStreamlit: overview / behavior / rules / Top-5 / performance\n\nRunner: ordered stages, PASS checks, resume, lock, exception propagation\nStorage deployed: LOCAL filesystem. HDFS/cluster: future architecture.',
          'src/run_pipeline.py; src/pipeline; app/data_loader.py',run_id,records)
    raw=read('raw_profile.json');silver=read('silver_profile.json');model=read('model_summary.json');eda=read('eda_summary.json')
    layers=[]
    for name,v in raw['tables'].items():layers.append(['Bronze',name,v['bronze_rows']])
    for name,v in silver['tables'].items():layers.append(['Silver',name,v['rows']])
    for name,v in model['exports'].items():layers.append(['Gold',name,v['rows']])
    layers.extend([['Gold','kpi_summary',1],['Gold','product_catalog',eda['kpi']['total_products']]])
    sheet('03_bronze_silver_gold.png','Các lớp dữ liệu và số dòng thật',pd.DataFrame(layers,columns=['Lớp','Bảng','Dòng']),'raw_profile.json; silver_profile.json; model_summary.json; Gold metadata',run_id,records)
    quality=pd.DataFrame([{'Bảng':n,'Dòng':v['row_count'],'Cột':v['column_count'],'Null khóa':sum(v['key_null_counts'].values()),'Duplicate':v['duplicate_excess_rows'],'Miền sai':sum(v['invalid_counts'].values()),'Trạng thái':v['status']} for n,v in raw['tables'].items()])
    sheet('04_raw_validation.png','Kiểm tra raw bằng Spark',quality,'outputs/metrics/raw_profile.json',run_id,records)
    reconciliation=pd.DataFrame([{'Bảng':n,'CSV':v['row_count'],'Bronze':v['bronze_rows'],'Đọc cuối':v['final_read_rows'],'Chênh lệch':v['bronze_rows']-v['row_count'],'Kết quả':v['reconciliation']} for n,v in raw['tables'].items()])
    sheet('05_row_reconciliation.png','Đối soát CSV và Bronze',reconciliation,'outputs/metrics/raw_profile.json',run_id,records)
    sheet('06_silver_quality.png','Chất lượng Silver và khóa ngoại',
          '\n'.join(f'{k}: {v}' for k,v in silver['foreign_keys'].items())+'\n\nBasket integrity: '+str(silver['basket_integrity'])+'\nStatus: '+silver['status'],
          'outputs/metrics/silver_profile.json',run_id,records)
    comparison=pd.DataFrame(model['comparisons'])[['config_id','status','frequent_itemsets','useful_rules','median_lift','coverage','hitrate_at_5','training_seconds']]
    for col in ['median_lift','coverage','hitrate_at_5','training_seconds']:comparison[col]=comparison[col].round(5)
    sheet('15_model_comparison.png','So sánh tham số FP-Growth thực tế',comparison,'outputs/metrics/model_summary.json; model_comparison.csv',run_id,records)
    rules=pq.read_table(ROOT/'data/gold/association_rules').to_pandas().sort_values(['lift','support'],ascending=False).head(10)
    top=pd.DataFrame({'Antecedent':rules.antecedent_names.map(lambda a:', '.join(a)[:48]),'Consequent':rules.consequent_names.map(lambda a:', '.join(a)[:45]),'Support':rules.support.round(6),'Confidence':rules.confidence.round(6),'Lift':rules.lift.round(4)})
    sheet('16_top_rules_table.png','Top luật: support ≥ 0,001',top,'data/gold/association_rules; sorted by lift/support',run_id,records)
    catalog=pq.read_table(ROOT/'data/gold/product_catalog').to_pandas();lookup=pq.read_table(ROOT/'data/gold/recommendation_lookup').to_pandas();popular=pd.read_csv(ROOT/'outputs/metrics/top_20_products.csv')
    basket_path=ROOT/'outputs/metrics/recommendation_evidence_basket.json'
    if not basket_path.exists():
        observed=None
        for file in sorted((ROOT/'data/silver/baskets').glob('*.parquet')):
            pf=pq.ParquetFile(file)
            if pf.metadata.num_rows==0:continue
            batch=next(pf.iter_batches(batch_size=256))
            for row in sorted(batch.to_pylist(),key=lambda r:r['basket_size']):
                prediction=recommend(row['items'],lookup,popular,catalog)
                if row['basket_size']<=10 and len(prediction)==5 and (prediction.source=='Luật kết hợp').all():observed=row;break
            if observed is not None:break
        if observed is None:raise ValueError('No qualifying real Top-5 basket in bounded evidence sample')
        basket_path.write_text(json.dumps(observed,indent=2)+'\n')
    observed=read('recommendation_evidence_basket.json')
    basket=observed['items'];example=recommend(basket,lookup,popular,catalog)
    assert len(example)==5 and (example.source=='Luật kết hợp').all()
    example.to_csv(ROOT/'outputs/tables/recommendation_example.csv',index=False)
    sheet('17_recommendation_example.png',f"Giỏ quan sát order_id={observed['order_id']} → Top-5",example[['product_name','score','support','confidence','lift']].round(6),'Silver baskets; app/recommender.py; Gold lookup; recommendation_example.csv',run_id,records)
    tests=read('test_summary.json')
    sheet('18_test_summary.png','Kết quả kiểm thử thật',pd.DataFrame(tests['suites']),'logs/integrated_tests.log; outputs/metrics/test_summary.json',run_id,records)
    # Actual Spark process and actual localhost UI, not HTML reconstruction.
    ready=FOLDER/'spark_session.json'
    stop=FOLDER/('.stop_'+run_id)
    if stop.exists():stop.unlink()
    if ready.exists():ready.rename(FOLDER/f'spark_session_previous_{int(time.time())}.json')
    with (ROOT/'logs/spark_evidence.log').open('w') as log:
        worker=subprocess.Popen([sys.executable,str(ROOT/'src/evidence/spark_ui_session.py'),run_id],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+120
            while not ready.exists():
                if worker.poll() is not None:raise RuntimeError('Spark evidence process failed; inspect spark_evidence.log')
                if time.monotonic()>deadline:raise TimeoutError('Spark UI action timeout')
                time.sleep(.5)
            with sync_playwright() as p:
                browser=p.chromium.launch(headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
                page=browser.new_page(viewport={'width':1600,'height':1100},device_scale_factor=1)
                for filename,route,label in [('07_spark_ui_jobs.png','jobs/','Spark UI Jobs sau action thật'),('08_spark_ui_stages.png','stages/','Spark UI Stages thật'),('09_spark_ui_environment.png','environment/','Spark UI Environment thật')]:
                    page.goto('http://127.0.0.1:4040/'+route,wait_until='networkidle')
                    if route=='environment/':
                        page.get_by_text('Spark Properties',exact=False).first.click()
                        page.get_by_text('spark.driver.memory',exact=True).wait_for(timeout=30000)
                        page.wait_for_timeout(2500)
                        page.set_viewport_size({'width':1900,'height':2200})
                        page.get_by_text('spark.driver.memory',exact=True).scroll_into_view_if_needed()
                        page.wait_for_timeout(500)
                    page.screenshot(path=str(FOLDER/filename),full_page=True)
                    records.append({'file':filename,'description':label,'source':'http://127.0.0.1:4040/'+route+'; src/evidence/spark_ui_session.py','kind':'actual browser screenshot','run_id':run_id,'execution_mode':mode,'created_at':timestamp()})
                browser.close()
        finally:
            (FOLDER/('.stop_'+run_id)).touch()
            try:worker.wait(timeout=20)
            except subprocess.TimeoutExpired:worker.terminate();worker.wait(timeout=10)
    import urllib.request
    try:
        urllib.request.urlopen(dashboard_url+'/_stcore/health',timeout=3)
    except OSError:
        with (ROOT/'logs/dashboard_server.log').open('a') as server_log:
            server=subprocess.Popen([sys.executable,'-m','streamlit','run','app/dashboard.py',
                '--server.address','127.0.0.1','--server.port',str(port),'--server.headless','true',
                '--browser.gatherUsageStats','false','--theme.primaryColor','#145C43'],cwd=ROOT,
                stdin=subprocess.DEVNULL,stdout=server_log,stderr=subprocess.STDOUT,start_new_session=True)
        deadline=time.monotonic()+30
        while True:
            try:urllib.request.urlopen(dashboard_url+'/_stcore/health',timeout=2);break
            except OSError:
                if server.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Dashboard server could not start')
                time.sleep(.5)
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1600,'height':1100},device_scale_factor=1)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(dashboard_url,wait_until='domcontentloaded')
        page.get_by_text('Instacart Basket Intelligence',exact=True).wait_for(timeout=60000)
        pages=[('10_dashboard_overview.png','Tổng quan'),('11_dashboard_behavior.png','Hành vi mua sắm'),('12_dashboard_rules.png','Luật kết hợp'),('13_dashboard_recommendation.png','Gợi ý sản phẩm'),('14_dashboard_performance.png','Hiệu năng hệ thống')]
        for filename,label in pages:
            page.set_viewport_size({'width':1600,'height':1100})
            page.get_by_text(label,exact=True).first.click()
            if label=='Gợi ý sản phẩm':
                names=dict(zip(catalog.product_id.astype(int),catalog.product_name))
                for product_id in basket:
                    selector=page.get_by_role('combobox').first;selector.fill(names[product_id])
                    page.get_by_role('option').filter(has_text=re.compile(r' · #'+str(product_id)+r'$')).click()
                page.get_by_text('5 sản phẩm từ luật kết hợp.',exact=True).wait_for(timeout=30000)
                page.keyboard.press('Escape')
                page.get_by_text('Gợi ý sản phẩm mua kèm',exact=True).click()
                page.locator('[data-testid="stDataFrame"]').wait_for(timeout=30000)
            else:
                page.wait_for_timeout(1500)
            # Capture all actual scroll content by enlarging the browser viewport.
            height=page.locator('[data-testid="stMain"]').evaluate('(el) => el.scrollHeight')
            page.set_viewport_size({'width':1600,'height':min(12000,max(1100,height+100))})
            page.wait_for_timeout(700)
            page.screenshot(path=str(FOLDER/filename),full_page=True)
            assert page.locator('[data-testid="stException"]').count()==0, label+' has an app exception'
            records.append({'file':filename,'description':'Dashboard thật: '+label,'source':dashboard_url+'; app/dashboard.py','kind':'actual browser screenshot; viewport enlarged to show complete scroll content','run_id':run_id,'execution_mode':mode,'created_at':timestamp()})
        assert not errors,errors
        browser.close()
    for item in records:
        path=FOLDER/item['file']
        with Image.open(path) as im:
            im.load();require_nonblank=im.width>=1000 and im.height>=600 and max(ImageStat.Stat(im.convert('RGB')).stddev)>10
            assert require_nonblank,'Blank/undersized evidence: '+str(path)
            item['width']=im.width;item['height']=im.height
        item['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();item['status']='PASS'
    manifest={'run_id':run_id,'execution_mode':mode,'created_at':timestamp(),'status':'PASS','images':records,
              'review':'Automated nonblank/size/content assertions; human visual review recorded separately.'}
    (FOLDER/'evidence_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    lines=['# Evidence Manifest','',f'run_id: {run_id}; execution_mode: {mode}.','', '| File | Nội dung | Loại | Nguồn | Thời gian | Trạng thái |','|---|---|---|---|---|---|']
    for item in records:lines.append(f"| [{item['file']}]({item['file']}) | {item['description']} | {item['kind']} | {item['source']} | {item['created_at']} | {item['status']} |")
    (FOLDER/'EVIDENCE_MANIFEST.md').write_text('\n'.join(lines)+'\n')
    print('Actual evidence captured:',len(records))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--run-id');args=parser.parse_args();capture(args.run_id or read('prototype_summary.json')['run_id'])
