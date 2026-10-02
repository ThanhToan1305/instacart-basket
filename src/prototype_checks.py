"""Independent artifact verification for pipeline resume and integration."""
import csv
import json
from pathlib import Path
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]


def report(name):
    return json.loads((ROOT/'outputs/metrics'/name).read_text())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parquet(relative, expected=None, success=True):
    path=ROOT/relative
    files=sorted(path.glob('*.parquet'))
    require(bool(files),f'Missing Parquet: {relative}')
    if success:require((path/'_SUCCESS').is_file(),f'Incomplete output: {relative}')
    rows=0;schema=None
    for file in files:
        pf=pq.ParquetFile(file)
        require(schema is None or schema.equals(pf.schema_arrow,check_metadata=False),f'Schema mismatch: {file}')
        schema=pf.schema_arrow;rows+=pf.metadata.num_rows
        for i in range(pf.metadata.num_row_groups):
            for j in range(pf.metadata.num_columns):
                require(pf.metadata.row_group(i).column(j).compression=='SNAPPY',f'Not Snappy: {file}')
        # Decode actual rows from every fragment without loading a large table.
        next(pf.iter_batches(batch_size=32),None)
    if expected is not None:require(rows==expected,f'Row mismatch: {relative}: {rows} != {expected}')
    return {'rows':rows,'columns':schema.names,'bytes':sum(f.stat().st_size for f in path.rglob('*') if f.is_file()),
            'files':len(files),'verification':'metadata counts + decoded sample from every fragment'}


def raw_check():
    from pipeline.raw_validation import inspect_files
    actual=inspect_files(ROOT/'data/raw');saved=report('raw_profile.json')
    require(saved['status']=='PASS','Raw report is not PASS')
    for name,v in actual.items():
        require(v['header_matches'],'Raw header mismatch: '+name)
        old=saved['tables'][name]
        require(v['size_bytes']==old['size_bytes'],'Raw size changed: '+name)
        require(all(old['checks'].values()),'Raw DQ failed: '+name)
    return {'input_rows':{n:v['row_count'] for n,v in saved['tables'].items()},
            'output_rows':{n:v['row_count'] for n,v in saved['tables'].items()}}


def bronze_check():
    saved=report('raw_profile.json');require(saved['status']=='PASS','Bronze report is not PASS')
    details={}
    for name,v in saved['tables'].items():
        require(v.get('reconciliation')=='PASS' and v.get('bronze_schema_matches') and v.get('snappy'),f'Invalid Bronze report: {name}')
        details[name]=parquet('data/bronze/'+name,v['row_count'])
    return {'input_rows':{n:v['row_count'] for n,v in saved['tables'].items()},
            'output_rows':{n:v['rows'] for n,v in details.items()},'tables':details}


def silver_check():
    saved=report('silver_profile.json');require(saved['status']=='PASS','Silver report is not PASS')
    require(all(v==0 for v in saved['foreign_keys'].values()),'Silver foreign key failure')
    require(saved['basket_integrity']['invalid']==0,'Invalid baskets')
    details={n:parquet('data/silver/'+n,v['rows']) for n,v in saved['tables'].items()}
    require(all(details[n]['columns']==saved['tables'][n]['columns'] for n in details),'Silver schema differs')
    return {'input_rows':{n:v['row_count'] for n,v in saved['bronze_validation'].items()},
            'output_rows':{n:v['rows'] for n,v in details.items()},'tables':details}


def eda_check():
    saved=report('eda_summary.json');require(saved['status']=='PASS','EDA report is not PASS')
    table=parquet('data/gold/kpi_summary',1)
    for chart in saved['charts']:
        for suffix in ['csv','png','html']:require(Path(chart[suffix]).is_file(),'Missing figure: '+chart[suffix])
        with Path(chart['csv']).open(newline='') as stream:
            require(len(list(csv.DictReader(stream)))==chart['rows'],'EDA CSV count differs')
    return {'input_rows':{'orders':saved['kpi']['total_orders'],'items':saved['kpi']['total_order_items']},
            'output_rows':{'kpi_summary':table['rows']},'tables':{'kpi_summary':table}}


def training_check():
    saved=report('model_summary.json')
    require(saved['status'] in ['PASS','TRAINED_AWAITING_EVALUATION'],'Training incomplete')
    rows=saved['comparisons'];require(len(rows)==9,'Incomplete configuration grid')
    require(len({(r['min_support'],r['min_confidence']) for r in rows})==9,'Duplicate grid configs')
    passed=[r for r in rows if r['status']=='PASS'];require(bool(passed),'No successful FP-Growth model')
    run=Path(saved['run_dir']);require(run.parent==ROOT/'data/gold','Invalid model checkpoint path')
    count=saved['preparation']['training_baskets']
    parquet(str(run.relative_to(ROOT)/'training_baskets'),count)
    for support in {r['min_support'] for r in passed}:
        row=next(r for r in passed if r['min_support']==support)
        parquet(str(run.relative_to(ROOT)/f's{support:g}'/'frequent_itemsets'),row['frequent_itemsets'])
        lowest=min((r for r in passed if r['min_support']==support),key=lambda r:r['min_confidence'])
        parquet(str(run.relative_to(ROOT)/f's{support:g}'/'rules'),lowest['association_rules'])
    return {'input_rows':{'prior_baskets':saved['preparation']['prior_baskets']},
            'output_rows':{'training_baskets':count,'successful_configurations':len(passed)}}


def evaluation_check():
    saved=report('model_summary.json');require(saved['status']=='PASS','Evaluation not PASS')
    c=saved['selected'];require(0<=c['hitrate_at_5']<=c['coverage']<=1,'Invalid model metrics')
    require(c['eligible_users']==saved['evaluation']['eligible_users'],'Evaluation denominator differs')
    details={n:parquet('data/gold/'+n,v['rows']) for n,v in saved['exports'].items()}
    require(details['association_rules']['rows']==c['useful_rules'],'Final rule count mismatch')
    user_path=Path(saved['evaluation']['per_user_path']).relative_to(ROOT)
    parquet(str(user_path),c['eligible_users'])
    for chart in saved['charts']:
        for suffix in ['png','html','csv']:require(Path(chart[suffix]).is_file(),'Missing model chart')
    return {'input_rows':{'train_users':c['eligible_users']},
            'output_rows':{n:v['rows'] for n,v in details.items()},'tables':details}


def gold_check():
    eda=eda_check();model=evaluation_check()
    tables={**eda['tables'],**model['tables']}
    catalog=ROOT/'data/gold/product_catalog'
    if catalog.exists():tables['product_catalog']=parquet('data/gold/product_catalog',report('eda_summary.json')['kpi']['total_products'],success=False)
    public={p.name for p in (ROOT/'data/gold').iterdir() if p.is_dir() and not p.name.startswith('.')}
    require(public==set(tables),'Unexpected or missing Gold tables: '+str(public^set(tables)))
    # Gold tables are bounded aggregates/dimensions: decode all rows independently.
    for name in tables:
        table=pq.read_table(ROOT/'data/gold'/name)
        require(table.num_rows==tables[name]['rows'],'Full Gold read mismatch: '+name)
    return {'input_rows':{n:v['rows'] for n,v in tables.items()},'output_rows':{n:v['rows'] for n,v in tables.items()},'tables':tables}


def dashboard_check():
    catalog=parquet('data/gold/product_catalog',report('eda_summary.json')['kpi']['total_products'],success=False)
    report('dashboard_manifest.json')
    for name in ['orders_by_hour','orders_by_dow','top_20_products','top_15_departments','basket_size_distribution','reorder_by_department','model_comparison']:
        require((ROOT/'outputs/metrics'/f'{name}.csv').is_file(),'Dashboard metrics missing: '+name)
    return {'input_rows':{'catalog':catalog['rows'],'rules':report('model_summary.json')['final_rule_count']},
            'output_rows':{'catalog':catalog['rows']}}


def supplement_check():
    saved=report('eda_summary.json');require('most_purchased_aisle_id' in saved['kpi'],'Missing aisle KPI')
    import pandas as pd
    data=pd.read_csv(ROOT/'outputs/metrics/orders_per_user_distribution.csv')
    require(int(data.user_count.sum())==saved['kpi']['total_users'],'Orders/user denominator differs')
    require(int((data.orders_per_user*data.user_count).sum())==saved['kpi']['total_orders'],'Orders/user row total differs')
    for index in range(1,13):require(bool(list((ROOT/'outputs/figures').glob(f'{index:02d}_*.png'))),'Missing numbered figure '+str(index))
    return {'input_rows':{'orders':saved['kpi']['total_orders']},'output_rows':{'orders_per_user_distribution':len(data)}}
