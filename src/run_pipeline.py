"""Ordered, locked orchestration with verified resume and atomic audit reports."""
import argparse
import fcntl
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path
import prototype_checks as checks

ROOT=Path(__file__).resolve().parents[1]
SUMMARY=ROOT/'outputs/metrics/prototype_summary.json'


def now():return datetime.now(timezone.utc).isoformat()


def atomic_json(path,value):
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    temp.replace(path)


def fingerprint():
    paths=sorted((ROOT/'data/raw').glob('*.csv'))+sorted((ROOT/'config').glob('*.yaml'))
    records=[]
    for p in paths:
        records.append([str(p.relative_to(ROOT)),p.stat().st_size,p.stat().st_mtime_ns,
                        hashlib.sha256(p.read_bytes()).hexdigest() if p.suffix=='.yaml' else None])
    return hashlib.sha256(json.dumps(records).encode()).hexdigest()


def execute_stage(name,command,checker,resume,summary,logger):
    stage={'name':name,'started_at':now(),'status':'RUNNING','command':command}
    summary['stages'].append(stage);atomic_json(SUMMARY,summary)
    start=time.perf_counter()
    try:
        can_resume=False
        try:
            result=checker();can_resume=True
        except (OSError,KeyError,ValueError) as exc:
            logger.info('%s requires execution or repair: %s',name,exc)
        if resume and can_resume:
            stage['status']='RESUMED'
            logger.info('%s verified; skip completed stage',name)
        elif command is None:
            result=checker();stage['status']='PASS'
        else:
            logger.info('START %s %s',name,command)
            stage['log']=str(ROOT/'logs'/f'integrated_{name}.log')
            with Path(stage['log']).open('w') as output:
                subprocess.run(command,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,check=True)
            result=checker();stage['status']='PASS'
        stage.update(result)
        return result
    except Exception as exc:
        stage.update(status='FAIL',error=repr(exc))
        logger.exception('Stage %s failed; remaining stages stopped',name)
        raise
    finally:
        stage['ended_at']=now();stage['seconds']=round(time.perf_counter()-start,3)
        atomic_json(SUMMARY,summary)
        logger.info('END %s status=%s seconds=%s',name,stage['status'],stage['seconds'])


def test_check():
    import re
    text=(ROOT/'logs/integrated_tests.log').read_text()
    match=re.search(r'Ran (\d+) tests in ([\d.]+)s\s+OK',text)
    checks.require(match is not None,'Tests did not complete with OK')
    return {'input_rows':{},'output_rows':{},'tests':int(match[1]),'test_seconds':float(match[2])}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resume',action=argparse.BooleanOptionalAction,default=True,
                        help='Default: verify and retain completed outputs. --no-resume never deletes existing data.')
    parser.add_argument('--mode',choices=['auto','full','sample'],default='auto')
    parser.add_argument('--sample-fraction',type=float,default=0.01)
    parser.add_argument('--execution-mode',choices=['full','sample'],help=argparse.SUPPRESS)
    parser.add_argument('--evidence-mode',action='store_true',help='Capture actual Spark UI, dashboard and experiment evidence after tests')
    args=parser.parse_args()
    run_id='prototype_'+uuid4().hex[:12]
    mode=args.execution_mode or args.mode
    if mode=='auto':
        import psutil
        mode='full' if (ROOT/'outputs/metrics/model_summary.json').exists() or psutil.virtual_memory().available>=6*1024**3 else 'sample'
    if mode=='sample' and args.execution_mode is None:
        from evidence.sample_pipeline import prepare
        target,manifest=prepare(args.sample_fraction,run_id)
        command=[sys.executable,str(target/'src/run_pipeline.py'),'--execution-mode','sample','--no-resume']
        if args.evidence_mode:command.append('--evidence-mode')
        subprocess.run(command,cwd=target,check=True)
        result=json.loads((target/'outputs/metrics/prototype_summary.json').read_text())
        manifest.update(status=result['status'],summary_path=str(target/'outputs/metrics/prototype_summary.json'))
        atomic_json(ROOT/'outputs/metrics/sample_integration.json',manifest)
        print('Sample pipeline PASS:',target)
        return
    from evidence.environment import record
    environment=record(mode,run_id)
    (ROOT/'logs').mkdir(exist_ok=True);SUMMARY.parent.mkdir(parents=True,exist_ok=True)
    with (ROOT/'logs/integrated_pipeline.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',
                            handlers=[logging.FileHandler(ROOT/'logs/integrated_pipeline.log'),logging.StreamHandler()])
        logger=logging.getLogger('prototype')
        current=fingerprint()
        if not args.resume:
            published=[p for base in ['data/bronze','data/silver','data/gold']
                       for p in (ROOT/base).glob('*') if p.is_dir() and not p.name.startswith('.')]
            checks.require(not published,'--no-resume requires a clean workspace. Existing published outputs will not be overwritten; use --resume.')
        if args.resume and SUMMARY.exists():
            old=json.loads(SUMMARY.read_text())
            checks.require(old.get('input_fingerprint')==current,'Raw/config changed since checkpoint. Restore matching inputs or rebuild in a separate workspace; existing outputs will not be overwritten.')
        summary={'status':'RUNNING','run_id':run_id,'execution_mode':mode,'environment':environment,'started_at':now(),'resume':args.resume,'input_fingerprint':current,
                 'fingerprint_scope':'raw names/sizes/mtime and configuration content; not a full raw content hash',
                 'stages':[],'notes':['RESUMED seconds measure current artifact verification only; original stage times remain in source reports.',
                                      'Existing outputs are adopted only after PASS reports and independent artifact checks.']}
        atomic_json(SUMMARY,summary);start=time.perf_counter()
        python=sys.executable
        try:
            stages=[('raw',[python,'src/pipeline/01_validate_raw.py'],checks.raw_check),
                    ('bronze',[python,'src/pipeline/02_ingest_bronze.py'],checks.bronze_check),
                    ('silver',[python,'src/pipeline/03_build_silver.py'],checks.silver_check),
                    ('eda',[python,'src/pipeline/04_eda.py'],checks.eda_check)]
            for name,command,checker in stages:execute_stage(name,command,checker,args.resume,summary,logger)
            training=[python,'src/pipeline/05_train_fpgrowth.py']
            if args.resume and (ROOT/'outputs/metrics/model_summary.json').exists():
                prior=checks.report('model_summary.json');run=Path(prior.get('run_dir',''))
                if run.parent==ROOT/'data/gold' and (run/'preparation.json').exists():
                    training+=['--resume-run',str(run)]
            execute_stage('fpgrowth',training,checks.training_check,args.resume,summary,logger)
            execute_stage('evaluation',[python,'src/pipeline/06_evaluate_rules.py'],checks.evaluation_check,args.resume,summary,logger)
            execute_stage('supplement',[python,'src/evidence/supplement.py',run_id],checks.supplement_check,args.resume,summary,logger)
            execute_stage('gold',None,checks.gold_check,False,summary,logger)
            execute_stage('dashboard_data',[python,'app/prepare_dashboard_data.py'],checks.dashboard_check,args.resume,summary,logger)
            # Recheck Gold after the optional catalog was published.
            summary['gold']=checks.gold_check()['tables']
            execute_stage('tests',[python,'-m','unittest','discover','-s','tests','-v'],test_check,False,summary,logger)
            import re
            log=(ROOT/'logs/integrated_tests.log').read_text()
            suites={}
            for module in re.findall(r'^test_\w+ \((test_\w+)\.',log,re.M):
                suites[module]=suites.get(module,0)+1
            tests=summary['stages'][-1]
            test_report={'run_id':run_id,'execution_mode':mode,'status':'PASS','count':tests['tests'],
                         'seconds':tests['test_seconds'],'log':'logs/integrated_tests.log',
                         'suites':[{'suite':name,'count':count,'status':'PASS'} for name,count in suites.items()]}
            atomic_json(ROOT/'outputs/metrics/test_summary.json',test_report)
            import pandas as pd
            (ROOT/'outputs/tables').mkdir(exist_ok=True)
            pd.DataFrame(test_report['suites']).to_csv(ROOT/'outputs/tables/test_summary.csv',index=False)
            summary['metrics']={'raw':checks.report('raw_profile.json')['tables'],
                                'kpi':checks.report('eda_summary.json')['kpi'],
                                'model_selected':checks.report('model_summary.json')['selected']}
            summary['status']='PASS'
            if args.evidence_mode:
                subprocess.run([python,'src/evidence/capture.py','--run-id',run_id],cwd=ROOT,check=True)
        except BaseException as exc:
            summary.update(status='FAIL',error=repr(exc));logger.exception('Integrated pipeline failed')
            raise
        finally:
            summary['ended_at']=now();summary['seconds']=round(time.perf_counter()-start,3)
            atomic_json(SUMMARY,summary)
            logger.info('Prototype %s total_seconds=%s',summary['status'],summary['seconds'])

if __name__=='__main__':main()
