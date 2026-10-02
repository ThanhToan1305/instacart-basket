"""Safety tests for orchestration; isolated audit files, no mock dataset."""
import logging
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import subprocess
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import run_pipeline

class RunnerSafetyTests(unittest.TestCase):
    def test_resume_verifies_and_never_launches_command(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);summary={'stages':[]}
            with patch.object(run_pipeline,'ROOT',root),patch.object(run_pipeline,'SUMMARY',root/'summary.json'):
                result=run_pipeline.execute_stage('verified',[sys.executable,'-c','raise RuntimeError("must not run")'],
                    lambda:{'input_rows':{},'output_rows':{}},True,summary,logging.getLogger('test'))
            self.assertEqual(summary['stages'][0]['status'],'RESUMED')
            self.assertIn('ended_at',summary['stages'][0])

    def test_failure_is_logged_and_exception_propagates(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'logs').mkdir();summary={'stages':[]}
            def unavailable():raise ValueError('No completed stage')
            with patch.object(run_pipeline,'ROOT',root),patch.object(run_pipeline,'SUMMARY',root/'summary.json'):
                with self.assertRaises(subprocess.CalledProcessError):
                    run_pipeline.execute_stage('failed',[sys.executable,'-c','raise RuntimeError("intentional safety test")'],
                        unavailable,False,summary,logging.getLogger('test'))
            self.assertEqual(summary['stages'][0]['status'],'FAIL')
            self.assertIn('RuntimeError',(root/'logs/integrated_failed.log').read_text())
            self.assertTrue((root/'summary.json').exists())

if __name__=='__main__':unittest.main()
