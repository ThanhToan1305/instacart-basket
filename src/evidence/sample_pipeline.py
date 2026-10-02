"""Deterministic real-CSV sample in an isolated complete pipeline workspace."""
import json,shutil,sys
from pathlib import Path
from uuid import uuid4
import pyarrow as pa
import pyarrow.csv as csv
import yaml
ROOT=Path(__file__).resolve().parents[2]

def prepare(fraction,run_id):
    if not 0<fraction<=1:raise ValueError('sample fraction must be in (0,1]')
    threshold=round(fraction*10000)
    if threshold<1:raise ValueError('Minimum sample fraction is 0.0001')
    target=ROOT/'outputs/sample_runs'/run_id;target.mkdir(parents=True)
    for folder in ['src','app','config','tests']:
        shutil.copytree(ROOT/folder,target/folder,ignore=shutil.ignore_patterns('__pycache__','*.docx'))
    shutil.copy2(ROOT/'run_pipeline.sh',target/'run_pipeline.sh')
    for folder in ['data/raw','data/bronze','data/silver','data/gold','outputs/metrics','outputs/figures','outputs/tables','outputs/evidence','logs','docs']:(target/folder).mkdir(parents=True,exist_ok=True)
    counts={}
    for filename in ['aisles.csv','departments.csv','products.csv','orders.csv','order_products__prior.csv','order_products__train.csv']:
        source=ROOT/'data/raw'/filename;dest=target/'data/raw'/filename
        if not filename.startswith(('orders','order_products')):
            shutil.copy2(source,dest);counts[filename]=sum(batch.num_rows for batch in csv.open_csv(source));continue
        reader=csv.open_csv(source,read_options=csv.ReadOptions(block_size=1024*1024))
        with csv.CSVWriter(dest,reader.schema) as writer:
            total=0
            for batch in reader:
                mask=pa.array([int(x)%10000<threshold for x in batch.column('order_id').to_pylist()])
                selected=batch.filter(mask);writer.write_batch(selected);total+=selected.num_rows
            counts[filename]=total
    config=yaml.safe_load((target/'config/settings.yaml').read_text());config['spark']['driver_memory']='1g'
    (target/'config/settings.yaml').write_text(yaml.safe_dump(config))
    config=yaml.safe_load((target/'config/model_config.yaml').read_text());config['fpgrowth'].update(driver_memory='1g',num_partitions=16,task_cpus=4,worker_timeout_seconds=300)
    (target/'config/model_config.yaml').write_text(yaml.safe_dump(config))
    manifest={'run_id':run_id,'execution_mode':'sample','sampling_rule':f'order_id % 10000 < {threshold}',
              'requested_fraction':fraction,'effective_bucket_fraction':threshold/10000,'workspace':str(target),
              'dimension_scope':'complete original dimensions','transaction_rows':counts}
    (target/'outputs/metrics/sampling.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return target,manifest
