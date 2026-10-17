"""User-disjoint deterministic basket-completion cohorts; Spark-only source rows."""
import hashlib
from hdfs_pipeline.helpers import require

SEED = 20261016


def user_split(user_id, seed=SEED):
    bucket = int(hashlib.sha256(f'instacart-user-v1:{user_id}:{seed}'.encode()).hexdigest()[:8], 16) % 10000
    return 'train' if bucket < 7000 else 'validation' if bucket < 8500 else 'test'


def split_items(user_id, order_id, items, seed=SEED):
    ordered = sorted(items, key=lambda item: (hashlib.sha256(
        f'instacart-item-v1:{user_id}:{order_id}:{item}:{seed}'.encode()).hexdigest(), item))
    count = max(1, min(len(ordered) - 1, len(ordered) // 2))
    require(len(ordered) >= 2 and len(set(items)) == len(items), 'Invalid source basket')
    return ordered[:count], ordered[count:]


def fingerprint(frame):
    from pyspark.sql import functions as F
    hashed = frame.select(F.xxhash64(*frame.columns).alias('h'))
    value = hashed.agg(F.count('*').alias('rows'), F.sum(F.col('h').cast('decimal(38,0)')).alias('sum'),
                       F.min('h').alias('min'), F.max('h').alias('max')).first().asDict()
    return {'algorithm': 'count,sum(decimal(xxhash64)),min,max; order-independent, not cryptographic',
            'values': {k: str(v) if k == 'sum' else v for k, v in value.items()}, 'schema': frame.schema.jsonValue()}


def build_split(baskets, orders, seed=SEED):
    from pyspark.sql import functions as F, Window
    from hdfs_pipeline.train import prepare_training
    # Performs source type/key/null/FK/domain validation before selecting cohorts.
    prepare_training(baskets, orders)
    users = orders.select('user_id').distinct()
    key = F.sha2(F.concat_ws(':', F.lit('instacart-user-v1'), F.col('user_id').cast('string'), F.lit(str(seed))), 256)
    users = users.withColumn('bucket', F.pmod(F.conv(F.substring(key, 1, 8), 16, 10).cast('long'), F.lit(10000)))
    users = users.withColumn('split', F.when(F.col('bucket') < 7000, 'train')
                             .when(F.col('bucket') < 8500, 'validation').otherwise('test')).localCheckpoint(eager=True)
    order_splits = orders.select('order_id', 'user_id', 'eval_set', 'order_number').join(users, 'user_id')
    fit, counts = prepare_training(baskets, orders, users.where("split='train'"))
    require(fit.join(users.where("split!='train'"), 'user_id').limit(1).count() == 0, 'Fit user leakage')
    eligible = baskets.where(F.size('items') >= 2).join(order_splits, ['order_id', 'user_id'])
    eligible = eligible.where(F.col('eval_set').isin('prior', 'train') & F.col('split').isin('validation', 'test'))
    rank = Window.partitionBy('user_id').orderBy(F.desc((F.col('eval_set') == 'train').cast('int')),
                                                F.desc('order_number'), F.desc('order_id'))
    chosen = eligible.withColumn('rank', F.row_number().over(rank)).where('rank=1').drop('rank')
    ordered = F.sort_array(F.transform('items', lambda item: F.struct(F.sha2(F.concat_ws(':',
        F.lit('instacart-item-v1'), F.col('user_id').cast('string'), F.col('order_id').cast('string'),
        item.cast('string'), F.lit(str(seed))), 256).alias('key'), item.alias('item'))))
    chosen = chosen.withColumn('ordered_items', F.transform(ordered, lambda x: x['item']))
    size = F.greatest(F.lit(1), F.least(F.size('items') - 1, F.floor(F.size('items') / 2).cast('int')))
    chosen = chosen.withColumn('context_items', F.slice('ordered_items', 1, size))
    chosen = chosen.withColumn('target_items', F.slice('ordered_items', size + 1, F.size('items'))).localCheckpoint(eager=True)
    invalid = ((F.size('context_items') == 0) | (F.size('target_items') == 0) |
               (F.size(F.array_intersect('context_items', 'target_items')) > 0) |
               (F.sort_array(F.array_union('context_items', 'target_items')) != F.sort_array('items')))
    require(chosen.where(invalid).count() == 0, 'Context/target leakage or reconstruction failure')
    require(chosen.select('user_id').distinct().count() == chosen.count(), 'Duplicate evaluation user')
    tables = {'users': users, 'order_splits': order_splits, 'training_baskets': fit}
    cohorts = {}
    for split in ('validation', 'test'):
        cases = chosen.where(F.col('split') == split)
        tables[split + '_contexts'] = cases.select('order_id', 'user_id', 'context_items')
        tables[split + '_targets'] = cases.select('order_id', 'user_id', 'target_items')
        tables[split + '_selected_baskets'] = cases.select('order_id', 'user_id', 'eval_set', 'order_number', 'items')
        n = cases.count()
        require(n > 0, 'No eligible ' + split + ' cases')
        cohorts[split] = {'eligible_cases': n, 'selected_sources': {r['eval_set']: r['count']
                         for r in cases.groupBy('eval_set').count().collect()}}
    # Only three user-split aggregates and two source categories are collected.
    cohorts['users'] = {r['split']: r['count'] for r in users.groupBy('split').count().collect()}
    cohorts['training'] = counts
    cohorts['eligible_users_total'] = baskets.where(F.size('items') >= 2).select('user_id').distinct().count()
    return tables, cohorts
