#!/usr/bin/env bash
set -euo pipefail

mkdir -p /data/name
if [ ! -f /data/name/current/VERSION ]; then
  if [ -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo 'ERROR: NameNode directory is nonempty but has no VERSION. Refusing to format.' >&2
    exit 1
  fi
  /opt/hadoop/bin/hdfs namenode -format -nonInteractive -clusterid instacart-basket
fi
exec /opt/hadoop/bin/hdfs namenode
