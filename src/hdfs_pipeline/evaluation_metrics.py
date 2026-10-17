"""Target-isolated distributed prediction and auditable macro metrics."""
import time
from app.evaluation_engine import RuleEngine, METHODS
from hdfs_pipeline.helpers import require
from hdfs_pipeline.publish import checked_read


def bounded_records(frame, maximum):
    require(frame.limit(maximum + 1).count() <= maximum, f'Bounded engine input exceeds {maximum}')
    return [row.asDict(recursive=True) for row in frame.collect()]


def load_engine(rules, popularity):
    return RuleEngine(bounded_records(rules.select('rule_id', 'antecedent', 'consequent', 'confidence', 'lift', 'support'), 20000),
                      bounded_records(popularity.select('product_id', 'basket_count'), 100000))


def summary(scores):
    from pyspark.sql import functions as F
    data = scores.groupBy('method').agg(
        F.count('*').alias('eligible_cases'), F.avg('hit').alias('hit_rate_at_5'),
        F.avg('precision_at_5').alias('precision_at_5'), F.avg('recall_at_5').alias('recall_at_5'),
        F.avg((F.col('recommendation_count') > 0).cast('double')).alias('recommendation_coverage'),
        F.avg((F.col('rule_count') > 0).cast('double')).alias('rule_coverage'),
        F.sum((F.col('recommendation_count') == 0).cast('long')).alias('zero_recommendation_cases'),
        F.sum((F.col('recommendation_count') < 5).cast('long')).alias('fewer_than_5_cases'),
        F.sum('rule_count').alias('rule_recommendations'), F.sum('popularity_count').alias('popularity_recommendations'))
    return bounded_records(data.orderBy('method'), 3)


def score_predictions(predictions, targets):
    from pyspark.sql import functions as F
    require(targets.select('order_id').distinct().count() == targets.count(), 'Duplicate target case')
    require(predictions.groupBy('order_id', 'method').count().where('count!=1').count() == 0, 'Duplicate predictions')
    require(predictions.select('order_id').distinct().count() == targets.count(), 'Eligible case denominator mismatch')
    require(predictions.join(targets, ['order_id', 'user_id'], 'left_anti').count() == 0, 'Predictions have no matching target')
    scored = predictions.join(targets, ['order_id', 'user_id'])
    scored = scored.withColumn('recommended_ids', F.transform('recommendations', lambda x: x['product_id']))
    require(scored.where((F.size('target_items') == 0) | (F.size('context_items') == 0) |
                         (F.size(F.array_intersect('context_items', 'target_items')) != 0) |
                         (F.size(F.array_intersect('recommended_ids', 'context_items')) != 0) |
                         (F.size('recommended_ids') != F.size(F.array_distinct('recommended_ids'))) |
                         (F.size('recommended_ids') > 5)).count() == 0, 'Invalid context/target/prediction leakage')
    scored = scored.withColumn('hit_items', F.array_intersect('recommended_ids', 'target_items'))
    scored = scored.withColumn('hit_count', F.size('hit_items')).withColumn('hit', (F.col('hit_count') > 0).cast('double'))
    scored = scored.withColumn('precision_at_5', F.col('hit_count') / F.lit(5.0))
    scored = scored.withColumn('recall_at_5', F.col('hit_count') / F.size('target_items'))
    scored = scored.withColumn('recommendation_count', F.size('recommendations'))
    scored = scored.withColumn('rule_count', F.size(F.filter('recommendations', lambda x: x['source'] == 'rule')))
    return scored.withColumn('popularity_count', F.size(F.filter('recommendations', lambda x: x['source'] == 'popularity')))


def evaluate_cases(spark, hdfs, suffix, contexts, targets, rules, popularity):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType, DoubleType, LongType
    engine = load_engine(rules, popularity)
    shared = spark.sparkContext.broadcast(engine)
    dtype = ArrayType(StructType([
        StructField('product_id', IntegerType()), StructField('source', StringType()),
        StructField('rule_id', StringType()), StructField('confidence', DoubleType()),
        StructField('lift', DoubleType()), StructField('support', DoubleType()), StructField('basket_count', LongType())]))
    predictor = F.udf(lambda items, method: shared.value.predict(items, method, 5), dtype)
    started = time.perf_counter()
    # The UDF receives only context items and method. Targets are not in this plan.
    predictions = contexts.select('order_id', 'user_id', 'context_items').withColumn(
        'method', F.explode(F.array(*[F.lit(method) for method in METHODS])))
    predictions = predictions.withColumn('recommendations', predictor('context_items', 'method'))
    predictions, prediction_info = hdfs.save(predictions, suffix + '/predictions', contexts.count() * 3)
    scores = score_predictions(predictions, targets)
    expected = summary(scores)
    saved, score_info = hdfs.save(scores, suffix + '/per_case_scores', prediction_info['rows'])
    actual = summary(saved)
    require(len(expected) == len(actual) == 3, 'Missing evaluated method')
    for a, b in zip(expected, actual):
        for key in a:
            require(abs(a[key] - b[key]) <= 1e-12 if isinstance(a[key], float) else a[key] == b[key],
                    'Metrics readback mismatch: ' + key)
    require(len({row['eligible_cases'] for row in actual}) == 1, 'Unequal method denominators')
    cold = targets.select(F.explode('target_items').alias('product_id')).join(popularity, 'product_id', 'left_anti').count()
    total = targets.agg(F.sum(F.size('target_items')).alias('n')).first()['n']
    seconds = round(time.perf_counter() - started, 3)
    shared.destroy()
    return {'methods': actual, 'predictions': prediction_info, 'per_case_scores': score_info,
            'cold_target_occurrences': cold, 'target_occurrences': total,
            'cold_target_exposure_rate': cold / total, 'batch_seconds': seconds,
            'timing_scope': 'Spark batch predict+write+score+readback; not per-request latency'}
