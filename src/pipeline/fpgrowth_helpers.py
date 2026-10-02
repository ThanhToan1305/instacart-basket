"""FP-Growth config, scalar rule metrics, and distributed rule matching."""
from pathlib import Path
import yaml
from pyspark.sql import functions as F, Window
from pyspark.sql.types import ArrayType, IntegerType
from common.spark_session import create_spark_session
from pipeline.silver_helpers import ROOT, require


def load_config():
    with (ROOT / 'config/model_config.yaml').open() as stream:
        return yaml.safe_load(stream)['fpgrowth']


def spark_session(name):
    config = load_config()
    # Scope memory/concurrency tuning to Task 4, leaving shared defaults unchanged.
    return create_spark_session(name, extra_configs={'spark.driver.memory': config.get('driver_memory','2g'),
        'spark.task.cpus': config['task_cpus'],
        'spark.sql.files.maxPartitionBytes': 1024 * 1024,
        'spark.sql.adaptive.coalescePartitions.enabled': 'false'})


def config_id(support, confidence):
    return f's{support:g}_c{confidence:g}'


def basket_invalid(df, min_size=1):
    require(isinstance(df.schema['items'].dataType, ArrayType) and
            isinstance(df.schema['items'].dataType.elementType, IntegerType), 'items must be ArrayType(IntegerType)')
    return df.where(F.col('items').isNull() | (F.size('items') < min_size) |
        (F.size('items') != F.size(F.array_distinct('items'))) |
        F.exists('items', lambda x: x.isNull() | (x <= 0)) |
        F.col('order_id').isNull() | F.col('user_id').isNull()).count()


def useful_rules(rules, confidence):
    return (rules.where((F.col('confidence') >= confidence) & (F.col('lift') > 1) &
                (F.size('antecedent') > 0) & (F.size('consequent') == 1) &
                (F.size(F.array_intersect('antecedent','consequent')) == 0))
            .withColumn('antecedent', F.sort_array('antecedent'))
            .withColumn('consequent', F.sort_array('consequent'))
            .dropDuplicates(['antecedent','consequent'])
            .withColumn('rule_id', F.sha2(F.to_json(F.struct('antecedent','consequent')), 256)))


def rule_metrics(rules):
    row = rules.agg(F.count('*').alias('association_rules'), F.avg('support').alias('avg_support'),
        F.avg('confidence').alias('avg_confidence'), F.avg('lift').alias('avg_lift'),
        F.expr('percentile(lift, 0.5)').alias('median_lift'),
        F.sum(F.when(F.col('lift') > 1, 1).otherwise(0)).alias('rules_lift_gt_1')).first().asDict()
    row['rules_lift_gt_1'] = row['rules_lift_gt_1'] or 0
    return row


def recommendations(contexts, rules, k):
    """Match every antecedent item, exclude context items, rank distinct products."""
    index = rules.select('config_id','rule_id','antecedent','consequent','confidence','lift','support')
    index = index.withColumn('needed',F.size('antecedent')).withColumn('seed',F.explode('antecedent'))
    seeds = contexts.select('user_id', F.explode('context_items').alias('seed'))
    matched = (seeds.join(index,'seed').groupBy('config_id','user_id','rule_id','consequent','confidence','lift','support','needed')
               .count().where(F.col('count') == F.col('needed'))
               .withColumn('product_id',F.element_at('consequent',1)))
    existing = seeds.select('user_id',F.col('seed').alias('product_id'))
    candidates = (matched.join(existing,['user_id','product_id'],'left_anti')
                  .groupBy('config_id','user_id','product_id')
                  .agg(F.max(F.struct('confidence','lift','support')).alias('score')))
    ranking = Window.partitionBy('config_id','user_id').orderBy(F.desc('score.confidence'),F.desc('score.lift'),F.desc('score.support'),'product_id')
    return candidates.withColumn('rank',F.row_number().over(ranking)).where(F.col('rank') <= k)
