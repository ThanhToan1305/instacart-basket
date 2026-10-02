"""Run isolated FP-Growth workers, safely skip failures, preserve real metrics."""
import csv
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common.logger import get_logger
from pipeline.silver_helpers import ROOT, require, write_json
from pipeline.fpgrowth_helpers import load_config, config_id


def write_comparison(rows):
    fields=list(dict.fromkeys(k for row in rows for k in row))
    with (ROOT/'outputs/metrics/model_comparison.csv').open('w',newline='',encoding='utf-8') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--resume-run')
    parser.add_argument('--background',action='store_true')
    parser.add_argument('--finish-task',action='store_true')
    args=parser.parse_args()
    job_status_path=ROOT/'logs/fpgrowth_job_status.json'
    if args.background:
        log_path=ROOT/'logs/fpgrowth_background.log'
        child_args=[arg for arg in sys.argv[1:] if arg!='--background']
        with log_path.open('a') as output:
            job=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),*child_args],
                cwd=ROOT,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        write_json(job_status_path,{'status':'TRAINING','pid':job.pid,'log':str(log_path)})
        print(f'Background job started: pid={job.pid}; log={log_path}')
        return
    cfg=load_config()
    logger=get_logger('fpgrowth_pipeline',ROOT/'logs/fpgrowth_pipeline.log')
    require(not (ROOT/'data/gold/association_rules').exists(),'Final rules already exist; refusing to overwrite')
    run=Path(args.resume_run).resolve() if args.resume_run else ROOT/'data/gold'/('.fpgrowth_run_'+uuid4().hex)
    if args.resume_run:
        require(run.parent == ROOT/'data/gold' and (run/'preparation.json').is_file(),'Invalid resume directory')
    else:
        run.mkdir()
    start=time.perf_counter()
    report={'status':'RUNNING','run_dir':str(run),'config':cfg,'comparisons':[]}
    previous_path=ROOT/'outputs/metrics/model_summary.json'
    previous=json.loads(previous_path.read_text()) if args.resume_run and previous_path.exists() else {}
    if args.resume_run:
        # Recover only workers belonging to this exact run, never unrelated Spark jobs.
        for cmdline in Path('/proc').glob('[0-9]*/cmdline'):
            try:
                tokens=cmdline.read_bytes().decode().split('\0')
                if len(tokens)>2 and tokens[1]==str(Path(__file__).resolve()) and '--resume-run' in tokens:
                    other_run=Path(tokens[tokens.index('--resume-run')+1]).resolve()
                    other_pid=int(cmdline.parent.name)
                    if other_run==run and other_pid!=os.getpid() and os.getpgid(other_pid)==other_pid:
                        os.killpg(other_pid,signal.SIGTERM)
                        logger.info('Stopped stale orchestrator pid=%s for this run',other_pid)
                if len(tokens)>2 and tokens[1]==str(ROOT/'src/pipeline/fpgrowth_worker.py') and '--run-dir' in tokens:
                    worker_run=Path(tokens[tokens.index('--run-dir')+1]).resolve()
                    pid=int(cmdline.parent.name)
                    if worker_run==run and os.getpgid(pid)==pid:
                        os.killpg(pid,signal.SIGTERM)
                        logger.info('Stopped stale isolated worker pid=%s for this run',pid)
            except (OSError,ValueError,UnicodeDecodeError):
                continue
    def worker(extra,label):
        path=ROOT/'logs'/f'fpgrowth_{run.name}_{label}_{uuid4().hex[:8]}.log'
        with path.open('w') as output:
            process=subprocess.Popen([sys.executable,str(ROOT/'src/pipeline/fpgrowth_worker.py'),'--run-dir',str(run),*extra],
                stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
            try:
                deadline=time.monotonic()+cfg['worker_timeout_seconds']
                while process.poll() is None:
                    with path.open('r') as check_log:
                        check_log.seek(max(0,path.stat().st_size-65536))
                        tail=check_log.read()
                    if 'OutOfMemoryError' in tail or 'Java heap space' in tail:
                        os.killpg(process.pid,signal.SIGTERM)
                        try:
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid,signal.SIGKILL)
                            process.wait()
                        logger.error('Worker %s OUT_OF_MEMORY; safely stopped; log=%s',label,path)
                        return False,'OUT_OF_MEMORY',str(path)
                    if time.monotonic()>deadline:
                        raise subprocess.TimeoutExpired(process.args,cfg['worker_timeout_seconds'])
                    time.sleep(1)
                code=process.returncode
            except subprocess.TimeoutExpired:
                # Kill the whole isolated group, including its Java gateway; no other worker is touched.
                os.killpg(process.pid,signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                    process.wait()
                logger.error('Worker %s TIMEOUT; group stopped; log=%s',label,path)
                return False,'TIMEOUT',str(path)
        logger.info('Worker %s exit=%s log=%s',label,code,path)
        return code==0,('PASS' if code==0 else 'FAIL'),str(path)
    try:
        if not args.resume_run:
            ok,status,log=worker(['--prepare'],'prepare')
            require(ok,f'Basket preparation {status}: {log}')
        report['preparation']=json.loads((run/'preparation.json').read_text())
        # Start with higher supports so resource-heavy 0.0005 cannot block useful alternatives.
        for support in sorted(cfg['min_supports'],reverse=True):
            folder=run/f's{support:g}'
            result_path=folder/'result.json'
            if args.resume_run and result_path.is_file():
                saved=json.loads(result_path.read_text())
                if saved.get('status')=='PASS' and all((folder/name/'_SUCCESS').is_file() for name in ('frequent_itemsets','rules')):
                    for row in saved['comparisons']:
                        prior_row=next((r for r in previous.get('comparisons',[]) if r['config_id']==row['config_id']),{})
                        row['worker_log']=prior_row.get('worker_log','')
                    report['comparisons'].extend(saved['comparisons'])
                    logger.info('Reused completed support=%s; no retraining',support)
                    continue
            if args.resume_run and support == 0.0005:
                # A confirmed heap failure remains evidence after a host restart.
                # Do not repeat the same resource-heavy fit when resuming.
                failed_log = next((p for p in sorted((ROOT/'logs').glob(f'fpgrowth_.fpgrowth_run_*_s{support:g}_*.log'))
                    if 'OutOfMemoryError' in p.read_text(errors='replace') or
                       'Java heap space' in p.read_text(errors='replace')), None)
                if failed_log is not None:
                    logger.warning('Skipped support=%s after confirmed prior OOM; log=%s',support,failed_log)
                    for confidence in cfg['min_confidences']:
                        report['comparisons'].append({'config_id':config_id(support,confidence),
                            'min_support':support,'min_confidence':confidence,'status':'SKIPPED_OUT_OF_MEMORY',
                            'error':'Confirmed heap failure in previous attempt; not retried on resume',
                            'worker_log':str(failed_log)})
                    continue
            if folder.exists():
                folder.rename(run/(folder.name+'_previous_'+uuid4().hex[:8]))
            logger.info('START support=%s (all 3 confidences)',support)
            ok,status,log=worker(['--support',str(support)],f's{support:g}')
            result_path=run/f's{support:g}'/'result.json'
            result=json.loads(result_path.read_text()) if result_path.exists() else {}
            if ok:
                for row in result['comparisons']:
                    row['worker_log']=log
                report['comparisons'].extend(result['comparisons'])
            else:
                for confidence in cfg['min_confidences']:
                    report['comparisons'].append({'config_id':config_id(support,confidence),'min_support':support,
                        'min_confidence':confidence,'status':'SKIPPED_'+status,'error':result.get('error',status),'worker_log':log})
            write_comparison(report['comparisons'])
            write_json(ROOT/'outputs/metrics/model_summary.json',report)
        require(any(r['status']=='PASS' for r in report['comparisons']),'No successful configuration')
        report['status']='TRAINED_AWAITING_EVALUATION'
    except Exception as exc:
        report.update(status='FAIL',error=str(exc))
        logger.exception('FP-Growth training failed')
        raise
    finally:
        report['training_pipeline_seconds']=round(time.perf_counter()-start,3)
        write_comparison(report['comparisons'])
        write_json(ROOT/'outputs/metrics/model_summary.json',report)
        logger.info('Training %s seconds=%s',report['status'],report['training_pipeline_seconds'])
    if args.finish_task:
        try:
            write_json(job_status_path,{'status':'EVALUATING','pid':os.getpid()})
            evaluation=subprocess.run([sys.executable,str(ROOT/'src/pipeline/06_evaluate_rules.py')],cwd=ROOT)
            require(evaluation.returncode==0,'Evaluation failed; inspect fpgrowth_background.log')
            write_json(job_status_path,{'status':'TESTING','pid':os.getpid()})
            with (ROOT/'logs/fpgrowth_tests.log').open('w') as output:
                tests=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-p','test_fpgrowth.py','-v'],
                    cwd=ROOT,stdout=output,stderr=subprocess.STDOUT)
            require(tests.returncode==0,'Unit tests failed; inspect fpgrowth_tests.log')
            write_json(job_status_path,{'status':'PASS','pid':os.getpid()})
        except Exception as exc:
            write_json(job_status_path,{'status':'FAIL','pid':os.getpid(),'error':str(exc)})
            raise


if __name__=='__main__':
    main()
