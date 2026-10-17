import os
import socket
import subprocess
import sys
from pathlib import Path


def main():
    role = sys.argv[1]
    port = 9000 if role == 'namenode' else 9866
    with socket.create_connection(('127.0.0.1', port), timeout=2):
        pass
    if role == 'datanode':
        hdfs = Path(os.environ.get('HADOOP_HOME', '/opt/hadoop')) / 'bin' / 'hdfs'
        report = subprocess.run(
            [str(hdfs), 'dfsadmin', '-report'], capture_output=True, text=True, timeout=12,
        )
        if report.returncode or 'Live datanodes (1)' not in report.stdout:
            return 1
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, subprocess.TimeoutExpired):
        sys.exit(1)
