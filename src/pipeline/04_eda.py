"""Real Spark KPIs and bounded aggregate exports for Vietnamese EDA figures."""
import os
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault('MPLCONFIGDIR', str(Path(__file__).resolve().parents[2] / 'logs/.matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from common.spark_session import create_spark_session
from common.logger import get_logger
from pipeline.silver_helpers import ROOT, stage, require, write_json, directory_bytes

COLOR = '#238b68'


def small_pandas(df, max_rows=500):
    require(df.limit(max_rows + 1).count() <= max_rows, 'Aggregate exceeds permitted driver export size')
    return df.toPandas()


def chart(name, data, category, value, title, xlabel, ylabel, horizontal=False):
    figures = ROOT / 'outputs/figures'
    metrics = ROOT / 'outputs/metrics'
    figures.mkdir(parents=True, exist_ok=True)
    data.to_csv(metrics / (name + '.csv'), index=False)
    fig, ax = plt.subplots(figsize=(12, 10 if horizontal else 6))
    labels = data[category].astype(str).tolist()
    values = data[value].tolist()
    if horizontal:
        ax.barh(labels, values, color=COLOR)
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=.2)
    else:
        ax.bar(data[category], values, color=COLOR)
        ax.grid(axis='y', alpha=.2)
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    ax.set_axisbelow(True)
    fig.tight_layout()
    png = figures / (name + '.png')
    fig.savefig(png, dpi=180)
    plt.close(fig)
    plot = px.bar(data, x=value if horizontal else category,
                  y=category if horizontal else value, orientation='h' if horizontal else 'v',
                  title=title, color_discrete_sequence=[COLOR],
                  labels={category: ylabel if horizontal else xlabel, value: xlabel if horizontal else ylabel})
    if horizontal:
        plot.update_layout(yaxis={'categoryorder': 'array', 'categoryarray': labels[::-1]}, height=800)
    plot.update_layout(template='plotly_white')
    plot.write_html(str(figures / (name + '.html')), include_plotlyjs=True, full_html=True)
    return {'name': name, 'rows': len(data), 'csv': str(metrics / (name + '.csv')),
            'png': str(png), 'html': str(figures / (name + '.html'))}


def main():
    logger = get_logger('eda_pipeline', ROOT / 'logs/eda_pipeline.log')
    start = time.perf_counter()
    report = {'status': 'RUNNING', 'charts': [], 'definitions': {
        'orders': 'All orders, including test orders without observed product details.',
        'baskets': 'Observed prior + train orders only; no empty synthetic test baskets.',
        'reorder_rate': 'Sum(reordered) / observed item rows (prior + train).',
        'days_between_orders': 'Average non-null days_since_prior_order, capped at 30 in source data.',
        'weekday': 'Anonymized order_dow codes 0–6; calendar weekday names unknown.',
        'median': 'Exact percentile(basket_size, 0.5), not an approximation.'}}
    spark = None
    try:
        gold = ROOT / 'data/gold/kpi_summary'
        require(not gold.exists(), 'Existing kpi_summary; refusing to overwrite')
        for name in ('orders','products','order_items','order_items_enriched','baskets'):
            require((ROOT / 'data/silver' / name / '_SUCCESS').is_file(), f'Missing Silver: {name}')
        spark = create_spark_session('InstacartEDA')
        tables = {name: spark.read.parquet(str(ROOT / 'data/silver' / name))
                  for name in ('orders','products','order_items','order_items_enriched','baskets')}
        orders, products, items, enriched, baskets = [tables[n] for n in ('orders','products','order_items','order_items_enriched','baskets')]
        with stage('kpi_aggregation', report, logger):
            order_metrics = orders.agg(F.count('*').alias('total_orders'), F.countDistinct('user_id').alias('total_users'),
                F.avg('days_since_prior_order').alias('avg_days_between_orders'),
                F.count('days_since_prior_order').alias('days_between_orders_observations')).first().asDict()
            item_metrics = items.agg(F.count('*').alias('total_order_items'), F.avg('reordered').alias('reorder_rate')).first().asDict()
            basket_metrics = baskets.agg(F.count('*').alias('observed_baskets'), F.avg('basket_size').alias('avg_basket_size'),
                F.expr('percentile(basket_size, 0.5)').alias('median_basket_size'), F.sum('basket_size').alias('basket_item_total')).first().asDict()
            require(basket_metrics['basket_item_total'] == item_metrics['total_order_items'], 'Basket/items reconciliation failed')
            kpi = {**order_metrics, **item_metrics, **basket_metrics, 'total_products': products.count()}
            kpi['avg_orders_per_user'] = kpi['total_orders'] / kpi['total_users']
            kpi['orders_without_observed_items'] = kpi['total_orders'] - kpi['observed_baskets']
        with stage('chart_aggregations', report, logger):
            hours = small_pandas(orders.groupBy('order_hour_of_day').count().orderBy('order_hour_of_day'))
            days = small_pandas(orders.groupBy('order_dow').count().orderBy('order_dow'))
            top_products = small_pandas(enriched.groupBy('product_id','product_name').count().orderBy(F.desc('count'), 'product_id').limit(20))
            top_departments = small_pandas(enriched.groupBy('department_id','department').count().orderBy(F.desc('count'), 'department_id').limit(15))
            sizes = small_pandas(baskets.groupBy('basket_size').count().orderBy('basket_size'))
            reorder = small_pandas(enriched.groupBy('department_id','department').agg(F.count('*').alias('item_count'),
                F.sum('reordered').alias('reordered_items'), F.avg('reordered').alias('reorder_rate'))
                .orderBy(F.desc('reorder_rate'), 'department_id'))
            reorder['reorder_percent'] = reorder['reorder_rate'] * 100
            peak_hours = hours.loc[hours['count'] == hours['count'].max(), 'order_hour_of_day'].astype(int).tolist()
            peak_days = days.loc[days['count'] == days['count'].max(), 'order_dow'].astype(int).tolist()
            kpi.update(peak_hour=peak_hours[0], peak_hour_orders=int(hours['count'].max()),
                       peak_order_dow=peak_days[0], peak_day_orders=int(days['count'].max()),
                       most_purchased_product_id=int(top_products.iloc[0]['product_id']),
                       most_purchased_product_name=str(top_products.iloc[0]['product_name']),
                       most_purchased_product_count=int(top_products.iloc[0]['count']))
            report['peak_hour_ties'] = peak_hours
            report['peak_day_ties'] = peak_days
            require(int(hours['count'].sum()) == kpi['total_orders'], 'Hour aggregation mismatch')
            require(int(days['count'].sum()) == kpi['total_orders'], 'Weekday aggregation mismatch')
            require(int(sizes['count'].sum()) == kpi['observed_baskets'], 'Basket histogram mismatch')
            require(int(reorder['item_count'].sum()) == kpi['total_order_items'], 'Department aggregation mismatch')
        with stage('render_six_charts', report, logger):
            specs = [
                ('orders_by_hour',hours,'order_hour_of_day','count','Số đơn hàng theo giờ','Giờ trong ngày (0–23)','Số đơn hàng (đơn)',False),
                ('orders_by_dow',days,'order_dow','count','Số đơn hàng theo mã ngày trong tuần','Mã ngày trong tuần (0–6)','Số đơn hàng (đơn)',False),
                ('top_20_products',top_products,'product_name','count','20 sản phẩm được mua nhiều nhất','Lượt sản phẩm trong đơn (lượt)','Sản phẩm',True),
                ('top_15_departments',top_departments,'department','count','15 ngành hàng có nhiều lượt mua nhất','Lượt sản phẩm trong đơn (lượt)','Ngành hàng',True),
                ('basket_size_distribution',sizes,'basket_size','count','Phân phối kích thước giỏ hàng quan sát được','Số sản phẩm duy nhất trong giỏ (sản phẩm)','Số giỏ hàng (giỏ)',False),
                ('reorder_by_department',reorder,'department','reorder_percent','Tỷ lệ mua lại theo ngành hàng','Tỷ lệ mua lại (%)','Ngành hàng',True)]
            for spec in specs:
                report['charts'].append(chart(*spec))
        with stage('export_gold_and_metrics', report, logger):
            fields = []
            for name, value in kpi.items():
                kind = StringType() if isinstance(value, str) else DoubleType() if isinstance(value, float) else LongType()
                fields.append(StructField(name, kind, False))
            df = spark.createDataFrame([kpi], StructType(fields))
            df.write.mode('errorifexists').option('compression','snappy').parquet(str(gold))
            require(spark.read.parquet(str(gold)).first().asDict() == kpi, 'Gold KPI readback mismatch')
            report['kpi'] = kpi
            report['gold_path'] = str(gold)
            report['gold_size_bytes'] = directory_bytes(gold)
            import csv
            with (ROOT / 'outputs/metrics/eda_summary.csv').open('w', newline='', encoding='utf-8') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(kpi))
                writer.writeheader()
                writer.writerow(kpi)
        report['status'] = 'PASS'
    except Exception as exc:
        report.update(status='FAIL', error=str(exc))
        logger.exception('EDA pipeline failed')
        raise
    finally:
        if spark is not None:
            spark.stop()
        report['total_seconds'] = round(time.perf_counter() - start, 3)
        write_json(ROOT / 'outputs/metrics/eda_summary.json', report)
        logger.info('EDA %s total_seconds=%s', report['status'], report['total_seconds'])


if __name__ == '__main__':
    main()
