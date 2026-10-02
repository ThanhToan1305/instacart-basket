"""Evaluate nine configurations on train, select and export rules plus figures."""
import csv
import importlib
import json
import math
import os
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ.setdefault('MPLCONFIGDIR',str(Path(__file__).resolve().parents[2]/'logs/.matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark import StorageLevel
from pyspark.sql import functions as F, Window
from common.logger import get_logger
from pipeline.silver_helpers import ROOT, require, stage, write_json, directory_bytes
from pipeline.fpgrowth_helpers import load_config, spark_session, useful_rules, recommendations, config_id, rule_metrics


def bounded(df, maximum=1000):
    require(df.limit(maximum+1).count()<=maximum,'Plot export exceeds bounded size')
    return df.toPandas()


def save_plot(name, fig, data, plot):
    folder=ROOT/'outputs/figures'
    fig.tight_layout()
    fig.savefig(folder/f'{name}.png',dpi=180)
    plt.close(fig)
    plot.update_layout(template='plotly_white')
    plot.write_html(str(folder/f'{name}.html'),include_plotlyjs=True)
    data.to_csv(ROOT/'outputs/metrics'/f'{name}.csv',index=False)
    return {'name':name,'png':str(folder/f'{name}.png'),'html':str(folder/f'{name}.html'),
            'csv':str(ROOT/'outputs/metrics'/f'{name}.csv'),'rows':len(data)}


def main():
    logger=get_logger('fpgrowth_pipeline',ROOT/'logs/fpgrowth_pipeline.log')
    summary_path=ROOT/'outputs/metrics/model_summary.json'
    summary=json.loads(summary_path.read_text())
    require(summary['status']=='TRAINED_AWAITING_EVALUATION','Training must finish before evaluation')
    cfg=load_config()
    start=time.perf_counter()
    spark=None
    try:
        for name in ('frequent_itemsets','association_rules','recommendation_lookup'):
            require(not (ROOT/'data/gold'/name).exists(),f'Existing output: {name}; refusing to overwrite')
        spark=spark_session('InstacartRuleEvaluation')
        run=Path(summary['run_dir'])
        with stage('evaluation_contexts',summary,logger):
            baskets=spark.read.parquet(str(ROOT/'data/silver/baskets'))
            orders=spark.read.parquet(str(ROOT/'data/silver/orders'))
            history=baskets.join(orders.where("eval_set='prior'").select('order_id','order_number'),'order_id')
            w=Window.partitionBy('user_id').orderBy(F.desc('order_number'),F.desc('order_id'))
            latest=(history.withColumn('rn',F.row_number().over(w)).where('rn=1')
                    .select('user_id',F.col('items').alias('context_items'),F.col('order_id').alias('context_order_id'),
                            F.col('order_number').alias('context_order_number')))
            ground=(baskets.join(orders.where("eval_set='train'").select('order_id','order_number'),'order_id')
                    .select('user_id',F.col('items').alias('ground_truth'),F.col('order_id').alias('train_order_id'),
                            F.col('order_number').alias('train_order_number')))
            require(ground.count()==ground.select('user_id').distinct().count(),'More than one train order per user')
            contexts=ground.join(latest,'user_id').persist(StorageLevel.DISK_ONLY)
            denominator=contexts.count()
            require(denominator>0,'No eligible train users')
            require(contexts.where(F.col('train_order_number')<=F.col('context_order_number')).count()==0,
                    'Train target must come after its prior context')
            summary['evaluation']={'eligible_users':denominator,'train_users':ground.count(),
                'context':'Last observed prior basket per user; full antecedent containment; exclude items already in context.',
                'truth':'All items in the user train order; training uses prior only.',
                'coverage_definition':'Users receiving >=1 recommendation / all eligible users.',
                'hitrate_at_5_definition':'Users with >=1 top-5 recommendation in train ground truth / all eligible users, including uncovered users.',
                'ranking':'Best matching rule per product, ordered by confidence desc, lift desc, support desc, product_id asc.',
                'limitation':'Train ground truth is also used for configuration selection; these are internal validation metrics, not an unbiased final test.'}
        with stage('match_all_configurations',summary,logger):
            combined=None
            for support in cfg['min_supports']:
                valid=[r for r in summary['comparisons'] if r['status']=='PASS' and r['min_support']==support]
                if not valid:
                    continue
                rules=spark.read.parquet(str(run/f's{support:g}'/'rules'))
                for row in valid:
                    selected=useful_rules(rules,row['min_confidence']).withColumn('config_id',F.lit(row['config_id']))
                    combined=selected if combined is None else combined.unionByName(selected)
            require(combined is not None,'No valid rules')
            combined=combined.persist(StorageLevel.DISK_ONLY)
            predictions=recommendations(contexts,combined,cfg['recommendation_k']).persist(StorageLevel.DISK_ONLY)
            total_metrics=predictions.groupBy('config_id').agg(F.countDistinct('user_id').alias('covered_users'),F.count('*').alias('recommendation_count'))
            hits=(predictions.join(contexts.select('user_id','ground_truth'),'user_id')
                  .where(F.array_contains(F.col('ground_truth'),F.col('product_id')))
                  .groupBy('config_id').agg(F.countDistinct('user_id').alias('hit_users')))
            results=bounded(total_metrics.join(hits,'config_id','left').fillna(0,subset=['hit_users'])).to_dict('records')
            by_id={r['config_id']:r for r in results}
            for row in summary['comparisons']:
                if row['status']!='PASS':
                    continue
                actual=by_id.get(row['config_id'],{})
                row['eligible_users']=denominator
                row['covered_users']=int(actual.get('covered_users',0))
                row['hit_users']=int(actual.get('hit_users',0))
                row['recommendation_count']=int(actual.get('recommendation_count',0))
                row['coverage']=row['covered_users']/denominator
                row['hitrate_at_5']=row['hit_users']/denominator
                row['conditional_hitrate_at_5']=row['hit_users']/row['covered_users'] if row['covered_users'] else 0.0
                # A disclosed multi-objective score, rather than maximizing the number of rules.
                row['selection_score']=(.4*row['hitrate_at_5']+.35*row['coverage']+.1*(row['avg_confidence'] or 0)
                    +.1*min((row['median_lift'] or 0)/5,1)+.05*min(row['useful_rules']/10000,1)
                    -.02*min(row['training_seconds']/cfg['worker_timeout_seconds'],1))
            candidates=[r for r in summary['comparisons'] if r['status']=='PASS' and r['useful_rules']>0 and r['coverage']>0]
            require(candidates,'No usable covering configuration')
            chosen=max(candidates,key=lambda r:(r['selection_score'],r['hitrate_at_5'],-r['training_seconds'],r['min_support']))
            summary['selected']=chosen.copy()
            summary['selection_method']='0.40*HitRate@5 + 0.35*Coverage + 0.10*avg_confidence + 0.10*min(median_lift/5,1) + 0.05*min(useful_rules/10000,1) - 0.02*min(training_seconds/timeout,1)'
        with stage('export_selected_model',summary,logger):
            final=combined.where(F.col('config_id')==chosen['config_id']).drop('config_id')
            products=spark.read.parquet(str(ROOT/'data/silver/products')).select('product_id','product_name')
            # Names are attached only now, after fitting/evaluation on integer IDs.
            for array_col in ('antecedent','consequent'):
                names=(final.select('rule_id',F.posexplode(array_col).alias('pos','product_id'))
                       .join(products,'product_id').groupBy('rule_id')
                       .agg(F.sort_array(F.collect_list(F.struct('pos','product_name'))).alias('mapped'))
                       .select('rule_id',F.transform('mapped',lambda x:x['product_name']).alias(array_col+'_names')))
                final=final.join(names,'rule_id')
            fi=spark.read.parquet(str(run/f's{chosen["min_support"]:g}'/'frequent_itemsets'))
            fi.write.option('compression','snappy').parquet(str(ROOT/'data/gold/frequent_itemsets'))
            final.write.option('compression','snappy').parquet(str(ROOT/'data/gold/association_rules'))
            final=spark.read.parquet(str(ROOT/'data/gold/association_rules'))
            final_count=final.count()
            require(final_count==chosen['useful_rules'],'Final rules export lost rows')
            summary['final_rule_metrics']=rule_metrics(final)
            lookup=(final.select('rule_id','antecedent','consequent','confidence','lift','support','antecedent_names','consequent_names')
                    .withColumn('antecedent_size',F.size('antecedent'))
                    .withColumn('antecedent_product_id',F.explode('antecedent'))
                    .withColumn('recommended_product_id',F.element_at('consequent',1)))
            lookup.write.option('compression','snappy').parquet(str(ROOT/'data/gold/recommendation_lookup'))
            selected_predictions=predictions.where(F.col('config_id')==chosen['config_id'])
            eval_rows=(selected_predictions.groupBy('user_id').agg(F.sort_array(F.collect_list(F.struct('rank','product_id'))).alias('ranked'))
                       .select('user_id',F.transform('ranked',lambda x:x['product_id']).alias('recommendations')))
            eval_rows=(contexts.join(eval_rows,'user_id','left')
                       .withColumn('recommendations',F.coalesce('recommendations',F.array().cast('array<int>')))
                       .withColumn('covered',F.size('recommendations')>0)
                       .withColumn('hit',F.size(F.array_intersect('recommendations','ground_truth'))>0))
            eval_path=run/'selected_user_evaluation'
            eval_rows.write.option('compression','snappy').parquet(str(eval_path))
            check=spark.read.parquet(str(eval_path)).agg(F.count('*').alias('eligible'),
                F.sum(F.col('covered').cast('long')).alias('covered'),F.sum(F.col('hit').cast('long')).alias('hits')).first()
            require(check['eligible']==denominator and check['covered']==chosen['covered_users'] and check['hits']==chosen['hit_users'],'User evaluation export mismatch')
            summary['evaluation']['per_user_path']=str(eval_path)
            summary['final_rule_count']=final_count
            summary['exports']={}
            for name in ('frequent_itemsets','association_rules','recommendation_lookup'):
                path=ROOT/'data/gold'/name
                summary['exports'][name]={'path':str(path),'rows':spark.read.parquet(str(path)).count(),'size_bytes':directory_bytes(path)}
        with stage('five_model_figures',summary,logger):
            import pandas as pd
            comparison=pd.DataFrame([r for r in summary['comparisons'] if r['status']=='PASS'])
            charts=[]
            for name,column,title,ylabel in [('model_rule_counts','useful_rules','Số luật hữu dụng theo cấu hình','Số luật (luật)'),
                  ('model_training_times','training_seconds','Thời gian huấn luyện theo cấu hình','Thời gian (giây)')]:
                fig,ax=plt.subplots(figsize=(12,6))
                ax.bar(comparison['config_id'],comparison[column],color='#238b68')
                ax.set(title=title,xlabel='Cấu hình support/confidence',ylabel=ylabel)
                ax.tick_params(axis='x',rotation=35)
                charts.append(save_plot(name,fig,comparison[['config_id',column]],px.bar(comparison,x='config_id',y=column,title=title,labels={'config_id':'Cấu hình',column:ylabel},color_discrete_sequence=['#238b68'])))
            histogram=bounded(final.withColumn('lift_bin',F.floor(F.log2('lift')).cast('int')).groupBy('lift_bin').count().orderBy('lift_bin'))
            histogram['interval']=histogram['lift_bin'].apply(lambda x:f'[{2**x:g}, {2**(x+1):g})')
            fig,ax=plt.subplots(figsize=(10,6)); ax.bar(histogram['interval'],histogram['count'],color='#238b68')
            ax.set(title='Phân phối lift của luật đã chọn',xlabel='Khoảng lift (không đơn vị)',ylabel='Số luật (luật)')
            charts.append(save_plot('model_lift_distribution',fig,histogram,px.bar(histogram,x='interval',y='count',title='Phân phối lift',labels={'interval':'Khoảng lift','count':'Số luật'},color_discrete_sequence=['#238b68'])))
            top=bounded(final.where(F.col('support')>=cfg['plot_min_support']).orderBy(F.desc('lift'),F.desc('support'),'rule_id').limit(20))
            top['label']=top['antecedent_names'].apply(lambda x:', '.join(x))+' → '+top['consequent_names'].apply(lambda x:', '.join(x))
            top['label']=[f'{rank}. {label}' for rank,label in enumerate(top['label'],1)]
            top.to_csv(ROOT/'outputs/metrics/top_rules_details.csv',index=False)
            fig,ax=plt.subplots(figsize=(16,12)); ax.barh(top['label'],top['lift'],color='#238b68'); ax.invert_yaxis()
            ax.set(title=f"Top 20 luật theo lift, support ≥ {cfg['plot_min_support']}",xlabel='Lift (không đơn vị)',ylabel='Luật kết hợp')
            charts.append(save_plot('model_top_20_rules',fig,top,px.bar(top,x='lift',y='label',orientation='h',title='Top luật theo lift có kiểm soát support',color_discrete_sequence=['#238b68'])))
            density=bounded(final.withColumn('confidence_bin',F.floor(F.col('confidence')*20)/20)
                .withColumn('log2_lift_bin',F.floor(F.log2('lift')*4)/4)
                .groupBy('confidence_bin','log2_lift_bin').agg(F.count('*').alias('rule_count')).orderBy('confidence_bin','log2_lift_bin'),maximum=5000)
            density['lift_bin']=2**density['log2_lift_bin']
            fig,ax=plt.subplots(figsize=(10,6)); points=ax.scatter(density['confidence_bin'],density['lift_bin'],s=density['rule_count'].clip(upper=500)*2+8,c=density['rule_count'],cmap='Greens',edgecolors='#238b68')
            ax.set_yscale('log'); ax.set(title='Confidence–lift: mật độ luật theo khoảng',xlabel='Confidence (tỷ lệ, khoảng 0,05)',ylabel='Lift (thang log, khoảng log₂ 0,25)')
            fig.colorbar(points,ax=ax,label='Số luật trong khoảng (luật)')
            charts.append(save_plot('model_confidence_lift',fig,density,px.scatter(density,x='confidence_bin',y='lift_bin',size='rule_count',color='rule_count',log_y=True,title='Confidence–lift: mật độ luật theo khoảng')))
            summary['charts']=charts
        summary['status']='PASS'
    except Exception as exc:
        summary.update(status='FAIL',error=str(exc))
        logger.exception('Rule evaluation failed')
        raise
    finally:
        if spark is not None:
            spark.stop()
        summary['evaluation_pipeline_seconds']=round(time.perf_counter()-start,3)
        write_json(summary_path,summary)
        train_module=importlib.import_module('pipeline.05_train_fpgrowth')
        train_module.write_comparison(summary['comparisons'])
        logger.info('Evaluation %s seconds=%s',summary['status'],summary['evaluation_pipeline_seconds'])


if __name__=='__main__':
    main()
