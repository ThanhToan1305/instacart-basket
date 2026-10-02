"""Real Spark aggregates and numbered publication figures."""
import sys,json,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common.spark_session import create_spark_session
from pyspark.sql import functions as F
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parents[2]

FIGURES=[('orders_by_hour','01_orders_by_hour','order_hour_of_day','count','Đơn hàng theo giờ','Giờ','Số đơn (đơn)'),
 ('orders_by_dow','02_orders_by_day','order_dow','count','Đơn hàng theo mã ngày','Mã ngày 0–6','Số đơn (đơn)'),
 ('top_20_products','03_top_20_products','product_name','count','20 sản phẩm phổ biến nhất','Sản phẩm','Lượt mua (lượt)'),
 ('top_15_departments','04_top_departments','department','count','Department phổ biến','Department','Lượt mua (lượt)'),
 ('basket_size_distribution','05_basket_size_distribution','basket_size','count','Phân phối kích thước giỏ','Sản phẩm trong giỏ','Số giỏ (giỏ)'),
 ('reorder_by_department','06_reorder_rate_by_department','department','reorder_rate','Tỷ lệ mua lại theo department','Department','Reorder (tỷ lệ)'),
 ('orders_per_user_distribution','07_orders_per_user_distribution','orders_per_user','user_count','Phân phối số đơn mỗi người','Đơn/người','Số người (người)'),
 ('model_rule_counts','08_rule_count_comparison','config_id','useful_rules','Số luật hữu dụng theo cấu hình','Cấu hình','Số luật (luật)'),
 ('model_training_times','09_training_time_comparison','config_id','training_seconds','Thời gian fit theo cấu hình','Cấu hình','Thời gian (giây)'),
 ('model_lift_distribution','10_lift_distribution','interval','count','Phân phối lift mô hình đã chọn','Khoảng lift','Số luật (luật)')]

def figures(run_id):
    mode=json.loads((ROOT/'outputs/metrics/prototype_summary.json').read_text()).get('execution_mode','full')
    folder=ROOT/'outputs/figures';tables=ROOT/'outputs/tables';tables.mkdir(exist_ok=True)
    for name,out,x,y,title,xlabel,ylabel in FIGURES:
        data=pd.read_csv(ROOT/'outputs/metrics'/f'{name}.csv');data.to_csv(tables/f'{out}.csv',index=False)
        horizontal=name in ['top_20_products','top_15_departments','reorder_by_department']
        fig,ax=plt.subplots(figsize=(12,9 if horizontal else 6))
        if horizontal:ax.barh(data[x].astype(str),data[y],color='#145C43');ax.invert_yaxis();ax.set(xlabel=ylabel,ylabel=xlabel)
        else:
            ax.bar(data[x].astype(str) if x in ['config_id','interval'] else data[x],data[y],color='#145C43');ax.set(xlabel=xlabel,ylabel=ylabel)
            if x=='config_id':ax.tick_params(axis='x',rotation=30)
        ax.set_title(title);ax.grid(axis='x' if horizontal else 'y',alpha=.15);ax.set_axisbelow(True)
        fig.text(.02,.01,f'Nguồn: outputs/metrics/{name}.csv | {mode} | run_id={run_id}',fontsize=8)
        fig.tight_layout(rect=(0,.035,1,1));fig.savefig(folder/f'{out}.png',dpi=220);plt.close(fig)
    top=pd.read_csv(ROOT/'outputs/metrics/model_top_20_rules.csv');top.to_csv(tables/'11_top_association_rules.csv',index=False)
    import textwrap
    labels=['\n'.join(textwrap.wrap(str(x),55)) for x in top['label']]
    fig,ax=plt.subplots(figsize=(16,14));ax.barh(range(len(top)),top['lift'],color='#145C43');ax.set_yticks(range(len(top)),labels);ax.invert_yaxis()
    ax.set(title='Top 20 luật theo lift; support ≥ 0,001',xlabel='Lift (không đơn vị)',ylabel='Luật kết hợp')
    fig.text(.02,.01,f'Nguồn: model_top_20_rules.csv | {mode} | run_id={run_id}',fontsize=8)
    fig.tight_layout(rect=(0,.03,1,1));fig.savefig(folder/'11_top_association_rules.png',dpi=220);plt.close(fig)
    density=pd.read_csv(ROOT/'outputs/metrics/model_confidence_lift.csv');density.to_csv(tables/'12_confidence_vs_lift.csv',index=False)
    fig,ax=plt.subplots(figsize=(12,6));p=ax.scatter(density.confidence_bin,density.lift_bin,s=density.rule_count*2+8,c=density.rule_count,cmap='Greens')
    ax.set_yscale('log');ax.set(title='Confidence–lift: mật độ luật theo khoảng',xlabel='Confidence (tỷ lệ; khoảng 0,05)',ylabel='Lift (log; khoảng log₂ 0,25)');fig.colorbar(p,ax=ax,label='Số luật')
    fig.text(.02,.01,f'Nguồn: model_confidence_lift.csv | {mode} | run_id={run_id}',fontsize=8)
    fig.tight_layout(rect=(0,.035,1,1));fig.savefig(folder/'12_confidence_vs_lift.png',dpi=220);plt.close(fig)
    for filename in ['raw_profile.csv','eda_summary.csv','model_comparison.csv']:
        import shutil
        shutil.copy2(ROOT/'outputs/metrics'/filename,tables/filename)
    quality=json.loads((ROOT/'outputs/metrics/silver_profile.json').read_text())
    (ROOT/'outputs/metrics/silver_quality.json').write_text(json.dumps(quality,ensure_ascii=False,indent=2)+'\n')
    import shutil
    shutil.copy2(ROOT/'logs/eda_pipeline.log',ROOT/'logs/silver_pipeline.log')

def aggregates(run_id):
    mode=json.loads((ROOT/'outputs/metrics/prototype_summary.json').read_text()).get('execution_mode','full')
    spark=create_spark_session('Instacart_'+mode+'_Supplement_'+run_id,extra_configs={'spark.driver.memory':'1g','spark.task.cpus':4})
    start=time.perf_counter()
    try:
        orders=spark.read.parquet(str(ROOT/'data/silver/orders'))
        distribution=orders.groupBy('user_id').count().withColumnRenamed('count','orders_per_user').groupBy('orders_per_user').agg(F.count('*').alias('user_count')).orderBy('orders_per_user')
        assert distribution.count()<=500
        data=distribution.toPandas();data.to_csv(ROOT/'outputs/metrics/orders_per_user_distribution.csv',index=False)
        eda=json.loads((ROOT/'outputs/metrics/eda_summary.json').read_text())
        assert int(data.user_count.sum())==eda['kpi']['total_users']
        assert int((data.orders_per_user*data.user_count).sum())==eda['kpi']['total_orders']
        detail=spark.read.parquet(str(ROOT/'data/silver/order_items_enriched'))
        aisles=detail.groupBy('aisle_id','aisle').count().orderBy(F.desc('count'),'aisle_id').limit(20).toPandas()
        aisles.to_csv(ROOT/'outputs/tables/top_20_aisles.csv',index=False)
        first=aisles.iloc[0];eda['kpi'].update(most_purchased_aisle_id=int(first.aisle_id),most_purchased_aisle_name=first.aisle,most_purchased_aisle_count=int(first['count']))
        original=ROOT/'data/gold/kpi_summary';staging=ROOT/'data/gold'/('.kpi_supplement_'+run_id)
        backup=ROOT/'data/gold'/('.kpi_before_'+run_id)
        updated=spark.read.parquet(str(original))
        for key in ['most_purchased_aisle_id','most_purchased_aisle_name','most_purchased_aisle_count']:
            updated=updated.withColumn(key,F.lit(eda['kpi'][key]))
        updated.write.option('compression','snappy').parquet(str(staging))
        assert spark.read.parquet(str(staging)).count()==1
        original.rename(backup)
        try:staging.rename(original)
        except BaseException:backup.rename(original);raise
        eda['gold_size_bytes']=sum(p.stat().st_size for p in original.rglob('*') if p.is_file())
        pd.DataFrame([eda['kpi']]).to_csv(ROOT/'outputs/metrics/eda_summary.csv',index=False)
        eda['supplement']={'run_id':run_id,'execution_mode':mode,'seconds':round(time.perf_counter()-start,3),'source':'data/silver/orders and order_items_enriched','distribution_rows':len(data)}
        (ROOT/'outputs/metrics/eda_summary.json').write_text(json.dumps(eda,ensure_ascii=False,indent=2)+'\n')
    finally:spark.stop()
    figures(run_id)

if __name__=='__main__':aggregates(sys.argv[1])
