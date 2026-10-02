"""Canonical entry point; retain the previous task-specific script."""
import runpy
from pathlib import Path
if __name__=='__main__':
    runpy.run_path(str(Path(__file__).with_name('03_build_thưsilver.py')),run_name='__main__')
