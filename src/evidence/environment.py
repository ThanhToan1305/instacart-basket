import os,sys,json,platform,subprocess,shutil,importlib.metadata
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import psutil
ROOT=Path(__file__).resolve().parents[2]

def record(mode,run_id):
    vm=psutil.virtual_memory();disk=shutil.disk_usage(ROOT)
    result={'run_id':run_id,'created_at':datetime.now(ZoneInfo('Asia/Bangkok')).isoformat(),
      'execution_mode':mode,'python':platform.python_version(),'python_executable':sys.executable,
      'venv':sys.prefix!=sys.base_prefix,'platform':platform.platform(),'logical_cpus':os.cpu_count(),
      'ram_total_bytes':vm.total,'ram_available_bytes':vm.available,'disk_free_bytes':disk.free,
      'java':subprocess.run(['java','-version'],text=True,capture_output=True,check=True).stderr.strip(),
      'packages':{k:importlib.metadata.version(k) for k in ['pyspark','streamlit','pandas','pyarrow','plotly','python-docx','playwright']},
      'mode_reason':'Existing reconciled full outputs are resumed without retraining; resource limits do not relabel completed full data as a sample.' if mode=='full' else 'Deterministic order_id sample in an isolated workspace; original full outputs retained.'}
    for folder in ['outputs/metrics','outputs/evidence']:(ROOT/folder).mkdir(exist_ok=True,parents=True)
    (ROOT/'outputs/metrics/environment.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    (ROOT/'outputs/evidence/environment.txt').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return result
