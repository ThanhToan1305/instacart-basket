"""Keep a real, action-completed Spark UI alive only during capture."""
import sys,json,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common.spark_session import create_spark_session
ROOT=Path(__file__).resolve().parents[2]
run_id=sys.argv[1];stop=ROOT/'outputs/evidence'/('.stop_'+run_id)
mode=json.loads((ROOT/'outputs/metrics/prototype_summary.json').read_text()).get('execution_mode','full')
spark=create_spark_session('Instacart_'+mode+'_'+run_id,extra_configs={'spark.driver.memory':'1g','spark.task.cpus':4,'spark.ui.port':4040,'spark.evidence.run_id':run_id,'spark.evidence.execution_mode':mode})
try:
    spark.sparkContext.setJobDescription(mode.upper()+': read real Silver baskets and Gold rules for evidence')
    counts={}
    for name,path in [('baskets','data/silver/baskets'),('association_rules','data/gold/association_rules')]:
        counts[name]=spark.read.parquet(str(ROOT/path)).count()
    ui=spark.sparkContext.uiWebUrl
    assert ui.endswith(':4040'),f'Port 4040 already occupied: {ui}'
    (ROOT/'outputs/evidence/spark_session.json').write_text(json.dumps({'run_id':run_id,'execution_mode':mode,'ui':ui,'actual_counts':counts,'action_completed_at':time.time()},indent=2)+'\n')
    deadline=time.monotonic()+600
    while not stop.exists() and time.monotonic()<deadline:time.sleep(.5)
finally:spark.stop()
