"""Add run_id to legacy Bronze with staging and retained originals."""
import sys,json,time
from pathlib import Path
from uuid import uuid4
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F
from common.spark_session import create_spark_session
from pipeline.raw_validation import write_reports
from common.logger import get_logger
ROOT=Path(__file__).resolve().parents[2]

def main():
    report=json.loads((ROOT/'outputs/metrics/raw_profile.json').read_text())
    run_id='metadata_'+uuid4().hex;stage=ROOT/'data/bronze'/('.metadata_'+run_id);backup=ROOT/'data/bronze'/('.before_'+run_id)
    logger=get_logger('bronze_pipeline',ROOT/'logs/bronze_pipeline.log');start=time.perf_counter()
    spark=create_spark_session('BronzeRunIdMigration',extra_configs={'spark.driver.memory':'1g','spark.task.cpus':4})
    try:
        changed=[]
        for name,v in report['tables'].items():
            original=ROOT/'data/bronze'/name;df=spark.read.parquet(str(original))
            if 'run_id' in df.columns:
                continue
            target=stage/name;df.withColumn('run_id',F.lit(run_id)).write.option('compression','snappy').parquet(str(target))
            assert spark.read.parquet(str(target)).count()==v['row_count']
            backup.mkdir(exist_ok=True);original.rename(backup/name)
            try:target.rename(original)
            except BaseException:
                (backup/name).rename(original);raise
            v['bronze_size_bytes']=sum(p.stat().st_size for p in original.rglob('*') if p.is_file())
            changed.append(name);logger.info('run_id migration table=%s rows=%s run_id=%s backup=%s',name,v['row_count'],run_id,backup/name)
        if changed:
            report['metadata_migration']={'run_id':run_id,'tables':changed,'seconds':round(time.perf_counter()-start,3),
                                         'backup':str(backup),'meaning':'run_id assigned during migration; original ingestion_time retained'}
            write_reports(report)
    finally:spark.stop()
if __name__=='__main__':main()
