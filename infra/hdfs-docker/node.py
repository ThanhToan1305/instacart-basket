"""Foreground HDFS node; initialize only an empty, fresh NameNode volume."""
import os
import subprocess
import sys
from pathlib import Path


def ensure_namespace(name_dir, format_namespace):
    name_dir.mkdir(parents=True, exist_ok=True)
    if (name_dir / 'current' / 'VERSION').is_file():
        print('Existing NameNode namespace kept; no format.', flush=True)
        return
    if any(name_dir.iterdir()):
        raise RuntimeError('NameNode directory is not empty and has no VERSION. Refusing to format. Inspect the volume; do not delete or reset it.')
    format_namespace()
    if not (name_dir / 'current' / 'VERSION').is_file():
        raise RuntimeError('NameNode initialization did not create current/VERSION.')


def main():
    role = sys.argv[1] if len(sys.argv) > 1 else 'namenode'
    if role not in {'namenode', 'datanode'}:
        raise SystemExit('Supported roles: namenode, datanode')
    java = Path(os.environ['JAVA_HOME']) / 'bin' / 'java'
    if not java.is_file():
        raise SystemExit(f'JDK 17 was not found at {java}; inspect the existing image before changing it.')
    version = subprocess.run([str(java), '-version'], capture_output=True, text=True, check=True)
    if 'version "17.' not in version.stderr + version.stdout:
        raise SystemExit('This Hadoop 3.5.0 service requires Java 17.')
    hdfs = Path(os.environ.get('HADOOP_HOME', '/opt/hadoop')) / 'bin' / 'hdfs'
    if role == 'namenode':
        ensure_namespace(
            Path('/hdfs/name'),
            lambda: subprocess.run([str(hdfs), 'namenode', '-format', '-nonInteractive'], check=True),
        )
    else:
        Path('/hdfs/data').mkdir(parents=True, exist_ok=True)
    print(f'Starting HDFS {role} in foreground.', flush=True)
    os.execv(str(hdfs), [str(hdfs), role])


if __name__ == '__main__':
    main()
