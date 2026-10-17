#!/usr/bin/env bash
# HDFS local cho Instacart. Khong sua bashrc, Java he thong, source hay output cu.
set -Eeuo pipefail
KIT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VERSION=3.5.0
DIST_NAME="hadoop-${VERSION}.tar.gz"
EXPECTED_SHA512=04ab94496cc00c8b7a28d03f6308eff8d2a4e7f37a9da5e8e086e4d6fc990e7a94d661908f6a6136039536efb362614b8aecdef185b5fb8ed588f0b152c7aa16
STATE="${INSTACART_HDFS_STATE:-$HOME/.local/share/instacart-hdfs}"
INSTALL_ROOT="${INSTACART_HDFS_INSTALL:-$HOME/.local/opt/instacart-hdfs}"
HADOOP_DIST="$INSTALL_ROOT/hadoop-$VERSION"
CONF="$STATE/conf"
die() { printf 'LOI: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || die "Thieu lenh $1. Xem README.md."; }
help_text() {
  cat <<'TXT'
Cach dung: bash infra/hdfs/hdfs.sh LENH [THAM_SO]
  check                 Kiem tra may, Java 17, RAM va dung luong
  setup                 Tai, xac minh SHA512, cai Hadoop va tao cau hinh rieng
  init                  Khoi tao filesystem MOI mot lan; tu choi ghi de du lieu
  start                 Khoi dong NameNode va DataNode tren loopback
  status                Bao cao HDFS
  smoke                 Thu Spark ghi/doc Parquet; can pyspark trong Python dang dung
  upload DATA_RAW        Nap dung 6 CSV; khong ghi de file da ton tai
  ls                    Liet ke /instacart tren HDFS
  stop                  Dung DataNode va NameNode
  logs                  Xem cuoi log .log cua Hadoop
Khong chay bang sudo. Khong co lenh reset/xoa du lieu.
TXT
}

find_java17() {
  local candidate output
  local -a choices=()
  if [[ -n "${INSTACART_JAVA_HOME:-}" ]]; then
    choices+=("$INSTACART_JAVA_HOME")
  else
    [[ -n "${JAVA_HOME:-}" ]] && choices+=("$JAVA_HOME")
    if command -v java >/dev/null; then
      choices+=("$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")")
    fi
    for candidate in /usr/lib/jvm/*; do choices+=("$candidate"); done
  fi
  for candidate in "${choices[@]}"; do
    [[ -x "$candidate/bin/java" ]] || continue
    output="$("$candidate/bin/java" -version 2>&1)"
    if [[ "$output" =~ version[[:space:]]+\"17\. ]]; then
      export JAVA_HOME="$candidate"; return
    fi
  done
  die 'Can JDK 17. Tren Ubuntu: sudo apt update; sau do sudo apt install openjdk-17-jdk. Khong can doi Java mac dinh.'
}

environment() {
  find_java17
  export HADOOP_HOME="$HADOOP_DIST"
  export HADOOP_CONF_DIR="$CONF"
  export HADOOP_LOG_DIR="$STATE/logs"
  export HADOOP_PID_DIR="$STATE/pids"
  export HADOOP_HEAPSIZE_MAX=256
}
ready() {
  [[ -f "$STATE/managed-by-instacart-kit" ]] || die 'Chua setup hoac duong dan khong thuoc bo script nay.'
  [[ -x "$HADOOP_DIST/bin/hdfs" && -f "$CONF/hdfs-site.xml" ]] || die 'Chua setup hoan tat.'
  environment
}
dfs() { "$HADOOP_DIST/bin/hdfs" "$@"; }

action="${1:-help}"
case "$action" in
  help|-h|--help) help_text; exit 0 ;;
esac
[[ "$EUID" -ne 0 ]] || die 'Hay chay bang tai khoan Ubuntu thuong, khong sudo.'

case "$action" in
check)
  printf 'Kien truc: %s\n' "$(uname -m)"
  printf 'Hadoop: %s\nState: %s\n' "$HADOOP_DIST" "$STATE"
  df -h "$HOME"
  free -h
  find_java17
  printf 'Java cho HDFS: %s\n' "$JAVA_HOME"
  "$JAVA_HOME/bin/java" -version
  if command -v ss >/dev/null; then
    printf '\nCac cong HDFS neu da duoc su dung:\n'
    ss -ltn | awk 'NR == 1 || $4 ~ /:(9000|9870|9864|9866|9867)$/'
  fi
  printf '\nCHECK: xong. Bo nay danh cho Ubuntu Linux x86_64.\n'
  ;;
setup)
  [[ "$(uname -m)" == x86_64 ]] || die 'Goi tai duoc khoa cho x86_64; may ARM can binary aarch64 va checksum khac.'
  for cmd in python3 curl tar sha512sum; do need "$cmd"; done
  find_java17
  if [[ -d "$STATE" && ! -f "$STATE/managed-by-instacart-kit" ]] &&
     [[ -n "$(find "$STATE" -mindepth 1 -print -quit)" ]]; then
    die 'Thu muc state da co du lieu khong thuoc bo script; chon thu muc rieng.'
  fi
  mkdir -p "$INSTALL_ROOT" "$STATE"
  printf '%s\n' 'instacart-hdfs-kit-v1' > "$STATE/managed-by-instacart-kit"
  if [[ ! -x "$HADOOP_DIST/bin/hdfs" ]]; then
    [[ ! -e "$HADOOP_DIST" ]] || die 'Thu muc cai dat da ton tai nhung khong hop le; kiem tra truoc khi tai lai.'
    free_kb="$(df -Pk "$INSTALL_ROOT" | awk 'END {print $4}')"
    (( free_kb >= 6 * 1024 * 1024 )) || die 'Can it nhat 6 GiB trong cho tai va giai nen; dataset can them dung luong.'
    archive="$INSTALL_ROOT/$DIST_NAME"
    if [[ ! -f "$archive" ]]; then
      curl --fail --location --retry 3 \
        "https://dlcdn.apache.org/hadoop/common/hadoop-$VERSION/$DIST_NAME" -o "$archive.part"
      mv -- "$archive.part" "$archive"
    fi
    printf '%s  %s\n' "$EXPECTED_SHA512" "$archive" | sha512sum --check -
    staging="$(mktemp -d "$INSTALL_ROOT/extract.XXXXXX")"
    tar -xzf "$archive" -C "$staging" --no-same-owner
    mv -- "$staging/hadoop-$VERSION" "$HADOOP_DIST"
    rmdir -- "$staging"
    rm -- "$archive"
  fi
  if [[ -e "$CONF/core-site.xml" || -e "$CONF/hdfs-site.xml" ]]; then
    printf 'Cau hinh da ton tai; giu nguyen, khong ghi de.\n'
    ready
    dfs version
    exit 0
  fi
  mkdir -p "$CONF" "$STATE/namenode" "$STATE/datanode" "$STATE/tmp" "$STATE/logs" "$STATE/pids"
  # Copy whichever Log4j configuration is shipped in the pinned distribution.
  for f in "$HADOOP_DIST"/etc/hadoop/log4j*.properties; do
    if [[ -f "$f" ]]; then cp -- "$f" "$CONF/"; fi
  done
  python3 - "$STATE" "$JAVA_HOME" <<'PY'
from pathlib import Path
import shlex
import sys
import xml.etree.ElementTree as ET
state = Path(sys.argv[1]).resolve()
conf = state / 'conf'
def write(name, values):
    root = ET.Element('configuration')
    for key, value in values.items():
        prop = ET.SubElement(root, 'property')
        ET.SubElement(prop, 'name').text = key
        ET.SubElement(prop, 'value').text = str(value)
    ET.indent(root)
    ET.ElementTree(root).write(conf / name, encoding='utf-8', xml_declaration=True)
write('core-site.xml', {
    'fs.defaultFS': 'hdfs://127.0.0.1:9000',
    'hadoop.tmp.dir': str(state / 'tmp'),
})
write('hdfs-site.xml', {
    'dfs.replication': '1',
    'dfs.namenode.name.dir': (state / 'namenode').as_uri(),
    'dfs.datanode.data.dir': (state / 'datanode').as_uri(),
    'dfs.namenode.rpc-address': '127.0.0.1:9000',
    'dfs.namenode.http-address': '127.0.0.1:9870',
    'dfs.datanode.address': '127.0.0.1:9866',
    'dfs.datanode.ipc.address': '127.0.0.1:9867',
    'dfs.datanode.http.address': '127.0.0.1:9864',
    'dfs.datanode.hostname': 'localhost',
    'dfs.client.use.datanode.hostname': 'true',
    'dfs.datanode.du.reserved': '1073741824',
})
(conf / 'hadoop-env.sh').write_text(
    'export JAVA_HOME=' + shlex.quote(sys.argv[2]) + '\n'
    'export HADOOP_HEAPSIZE_MAX=256\n', encoding='utf-8')
PY
  ready
  dfs version
  printf '\nSETUP: xong. Tiep theo: bash infra/hdfs/hdfs.sh init\n'
  ;;
init)
  ready
  if [[ -f "$STATE/namenode/current/VERSION" ]]; then
    printf 'Da khoi tao. Khong format lai.\n'; exit 0
  fi
  for directory in "$STATE/namenode" "$STATE/datanode"; do
    [[ -z "$(find "$directory" -mindepth 1 -print -quit)" ]] || die "$directory khong rong; tu choi format."
  done
  dfs namenode -format -nonInteractive
  [[ -f "$STATE/namenode/current/VERSION" ]] || die 'Chua tao duoc VERSION; xem log.'
  printf 'INIT: xong. Khong format lai khi khoi dong nhung lan sau.\n'
  ;;
start)
  ready
  [[ -f "$STATE/namenode/current/VERSION" ]] || die 'Chay init truoc.'
  dfs --daemon status namenode >/dev/null 2>&1 || dfs --daemon start namenode
  dfs --daemon status datanode >/dev/null 2>&1 || dfs --daemon start datanode
  for attempt in {1..12}; do
    if report="$(dfs dfsadmin -report 2>&1)" && [[ "$report" == *'Live datanodes (1)'* ]]; then
      dfs dfsadmin -safemode wait
      dfs dfs -mkdir -p /instacart/raw /instacart/bronze /instacart/silver /instacart/gold /instacart/smoke
      printf '%s\n' "$report"
      printf '\nSTART: xong. UI: http://localhost:9870\n'
      exit 0
    fi
    sleep 2
  done
  die 'Chua thay 1 DataNode song. Chay lenh logs; khong format lai.'
  ;;
status)
  ready
  dfs --daemon status namenode
  dfs --daemon status datanode
  dfs dfsadmin -report
  ;;
stop)
  ready
  dfs --daemon stop datanode
  dfs --daemon stop namenode
  printf 'STOP: xong. Du lieu van duoc giu.\n'
  ;;
logs)
  ready
  shopt -s nullglob
  logfiles=("$STATE"/logs/*.log)
  (( ${#logfiles[@]} > 0 )) || die 'Chua co file log.'
  tail -n 60 -- "${logfiles[@]}"
  ;;
ls)
  ready
  dfs dfs -ls -R /instacart
  ;;
smoke)
  ready
  python_bin="${INSTACART_PYTHON:-python}"
  command -v "$python_bin" >/dev/null || die 'Kich hoat .venv cua du an hoac dat INSTACART_PYTHON.'
  "$python_bin" "$KIT_DIR/spark_smoke.py"
  ;;
upload)
  ready
  [[ $# -eq 2 ]] || die 'Cach dung: bash infra/hdfs/hdfs.sh upload data/raw'
  raw_dir="$(realpath -- "$2")"
  names=(aisles.csv departments.csv products.csv orders.csv order_products__prior.csv order_products__train.csv)
  total_bytes=0
  for filename in "${names[@]}"; do
    [[ -s "$raw_dir/$filename" ]] || die "Thieu hoac rong: $filename"
    total_bytes=$((total_bytes + $(stat -c %s -- "$raw_dir/$filename")))
  done
  free_kb="$(df -Pk "$STATE" | awk 'END {print $4}')"
  (( free_kb * 1024 >= total_bytes + 2 * 1024 * 1024 * 1024 )) || die 'Khong du cho ban CSV HDFS va 2 GiB du phong. Parquet can them cho rieng.'
  dfs dfs -mkdir -p /instacart/raw
  for filename in "${names[@]}"; do
    if dfs dfs -test -e "/instacart/raw/$filename"; then
      die "$filename da ton tai tren HDFS. Doi soat truoc; script khong ghi de."
    fi
  done
  for filename in "${names[@]}"; do
    dfs dfs -put "$raw_dir/$filename" "/instacart/raw/$filename"
    local_size="$(stat -c %s -- "$raw_dir/$filename")"
    hdfs_size="$(dfs dfs -stat '%b' "/instacart/raw/$filename")"
    [[ "$local_size" == "$hdfs_size" ]] || die "Sai kich thuoc: $filename"
    printf 'SIZE_OK %s %s bytes\n' "$filename" "$local_size"
  done
  dfs dfs -ls /instacart/raw
  printf 'UPLOAD: xong. Moi doi soat kich thuoc; pipeline can doi soat schema va so dong.\n'
  ;;
*) help_text; die "Khong ho tro lenh: $action" ;;
esac
