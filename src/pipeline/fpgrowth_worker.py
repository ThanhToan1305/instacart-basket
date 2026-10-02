"""Isolated JVM worker: preparation or one support level (three confidences)."""
import argparse
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
from common.logger import get_logger
from pipeline.silver_helpers import ROOT, require, write_json
from pipeline.fpgrowth_helpers import load_config, spark_session, basket_invalid, config_id, rule_metrics, useful_rules


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-dir',required=True)
    parser.add_argument('--support',type=float)
    parser.add_argument('--prepare',action='store_true')
    args=parser.parse_args()
    run=Path(args.run_dir)
    cfg=load_config()
    logger=get_logger('fpgrowth_pipeline', ROOT/'logs/fpgrowth_pipeline.log')
    spark=spark_session('InstacartFPGrowthWorker')
    started=time.perf_counter()
    report={'status':'RUNNING'}
    target=run/'preparation.json' if args.prepare else run/f's{args.support:g}'/'result.json'
    try:
        if args.prepare:
            baskets=spark.read.parquet(str(ROOT/'data/silver/baskets'))
            require(basket_invalid(baskets)==0,'Invalid source baskets')
            orders=spark.read.parquet(str(ROOT/'data/silver/orders'))
            prior=baskets.join(orders.where("eval_set='prior'").select('order_id'),'order_id')
            all_prior=prior.count()
            eligible=prior.where(F.size('items') >= cfg['min_basket_size']).select('order_id','user_id','items')
            count=eligible.count()
            require(count>0 and basket_invalid(eligible,cfg['min_basket_size'])==0,'Invalid training baskets')
            eligible.repartition(cfg['num_partitions']).write.option('compression','snappy').parquet(str(run/'training_baskets'))
            report.update(status='PASS',source_baskets=baskets.count(),prior_baskets=all_prior,training_baskets=count,
                          excluded_single_item_baskets=all_prior-count, training_eval_set='prior',training_seconds=round(time.perf_counter()-started,3))
        else:
            folder=target.parent
            train=spark.read.parquet(str(run/'training_baskets')).select('items')
            training_rows=train.count()
            fit_start=time.perf_counter()
            logger.info('START fit support=%s baskets=%s partitions=%s',args.support,training_rows,cfg['num_partitions'])
            model=FPGrowth(itemsCol='items',minSupport=args.support,minConfidence=min(cfg['min_confidences']),numPartitions=cfg['num_partitions']).fit(train)
            itemsets=model.freqItemsets.persist(StorageLevel.DISK_ONLY)
            itemset_count=itemsets.count()
            logger.info('Materialized frequent itemsets support=%s count=%s',args.support,itemset_count)
            require(itemset_count<=cfg['max_itemsets'],f'Safety cap: {itemset_count} itemsets > {cfg["max_itemsets"]}')
            rules=model.associationRules.persist(StorageLevel.DISK_ONLY)
            rule_count=rules.count()
            require(rule_count<=cfg['max_rules'],f'Safety cap: {rule_count} rules > {cfg["max_rules"]}')
            train_seconds=round(time.perf_counter()-fit_start,3)
            export_start=time.perf_counter()
            itemsets.write.option('compression','snappy').parquet(str(folder/'frequent_itemsets'))
            rules.write.option('compression','snappy').parquet(str(folder/'rules'))
            # minConfidence does not affect frequent pattern fitting: reuse the same fit.
            comparisons=[]
            for confidence in cfg['min_confidences']:
                filtered=rules.where(F.col('confidence')>=confidence)
                metrics=rule_metrics(filtered)
                useful_count=useful_rules(filtered,confidence).count()
                comparisons.append({'config_id':config_id(args.support,confidence),'status':'PASS',
                    'driver_memory':spark.sparkContext.getConf().get('spark.driver.memory'),
                    'num_partitions':cfg['num_partitions'],
                    'min_support':args.support,'min_confidence':confidence,'training_baskets':training_rows,
                    'frequent_itemsets':itemset_count,**metrics,'useful_rules':useful_count,
                    'training_seconds':train_seconds})
            export_seconds=round(time.perf_counter()-export_start,3)
            for row in comparisons:
                row['export_seconds']=export_seconds
            report.update(status='PASS',min_support=args.support,comparisons=comparisons,
                          training_seconds=train_seconds,export_seconds=export_seconds)
            itemsets.unpersist()
            rules.unpersist()
        logger.info('Worker PASS support=%s seconds=%s',args.support,time.perf_counter()-started)
    except Exception as exc:
        report.update(status='FAIL',error=str(exc))
        logger.exception('Worker FAIL support=%s',args.support)
        raise
    finally:
        report['total_seconds']=round(time.perf_counter()-started,3)
        write_json(target,report)
        spark.stop()


if __name__=='__main__':
    main()
