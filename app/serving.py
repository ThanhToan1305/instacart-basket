"""Spark-free validation of an immutable HDFS serving bundle."""
import hashlib
import json
import re
from pathlib import Path

TABLES = ('product_catalog', 'kpi_summary', 'association_rules', 'recommendation_lookup')
MAX_ROWS = 100_000


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read_manifest(root):
    root = Path(root).resolve()
    try:
        manifest = json.loads((root / 'serving_manifest.json').read_text(encoding='utf-8'))
        for key in ('etl_run_id', 'model_run_id', 'publication_id', 'export_id'):
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', manifest[key]):
                raise ValueError('Run ID không hợp lệ: ' + key)
        if not isinstance(manifest['created_at'], str) or not isinstance(manifest['model_config'], dict):
            raise ValueError('Thiếu thời điểm/cấu hình export.')
        if not isinstance(manifest['files'], dict) or not all(
                isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) for value in manifest['files'].values()):
            raise ValueError('Danh sách checksum không hợp lệ.')
        evaluated = manifest['evaluation_status'] == 'EVALUATED_BASKET_COMPLETION'
        if manifest['status'] != 'PASS' or manifest['evaluation_status'] not in ('NOT_EVALUATED', 'EVALUATED_BASKET_COMPLETION'):
            raise ValueError('Export chưa PASS hoặc giao thức đánh giá chưa được hỗ trợ.')
        if manifest['mode'] != 'Gold export từ HDFS' or manifest['execution_mode'] != 'SAMPLE':
            raise ValueError('Sai chế độ serving của pilot HDFS.')
        base = manifest['etl_root']
        model_root = base + '/models/' + manifest['model_run_id']
        if not base.startswith('hdfs://') or base.rsplit('/', 1)[-1] != manifest['etl_run_id']:
            raise ValueError('ETL lineage không hợp lệ.')
        if evaluated:
            evaluation_id = manifest['evaluation_run_id']
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', evaluation_id):
                raise ValueError('Evaluation ID không hợp lệ.')
            evaluation_root = base + '/evaluations/' + evaluation_id
            if manifest['evaluation_root'] != evaluation_root:
                raise ValueError('Evaluation root không khớp ETL.')
            if manifest['gold_root'] != evaluation_root + '/work/' + manifest['publication_id'] + '/gold':
                raise ValueError('Gold evaluation lineage không hợp lệ.')
            if (not manifest['train_popularity_uri'].startswith(evaluation_root + '/work/split_') or
                    not manifest['train_popularity_uri'].endswith('/train_popularity') or
                    '..' in manifest['train_popularity_uri'].split('/')):
                raise ValueError('Popularity phải lấy từ split train của evaluation này.')
            summary_path = root / 'outputs/metrics/evaluation_summary.json'
            verify_file(root, summary_path, manifest)
            evaluation = json.loads(summary_path.read_text(encoding='utf-8'))
            if evaluation['locked_config']['engine_sha256'] != sha256(Path(__file__).parent / 'evaluation_engine.py'):
                raise ValueError('Engine hiện tại khác fingerprint đã khóa; không thể demo model này.')
        for name in TABLES + (('train_popularity',) if evaluated else ()):
            entry = manifest['tables'][name]
            expected = (base + '/gold/' + name if name in ('product_catalog', 'kpi_summary')
                        else model_root + '/gold/' + manifest['publication_id'] + '/' + name)
            if evaluated and name not in ('product_catalog', 'kpi_summary'):
                expected = manifest['train_popularity_uri'] if name == 'train_popularity' else manifest['gold_root'] + '/' + name
            if (entry['uri'] != expected or type(entry['rows']) is not int or
                    entry['rows'] > MAX_ROWS or entry['rows'] < 0 or not isinstance(entry['arrow_schema'], str)):
                raise ValueError('Lineage/count vượt giới hạn 100.000 dòng: ' + name)
            folder = root / 'data/gold' / name
            if not folder.is_dir() or not (folder / '_SUCCESS').is_file():
                raise ValueError('Thiếu bảng hoàn tất: ' + name)
            if not list(folder.glob('*.parquet')):
                raise ValueError('Thiếu Parquet: ' + name)
        if not manifest['files']:
            raise ValueError('Thiếu danh sách file đối soát.')
        return manifest
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise ValueError('Export HDFS thiếu/không hợp lệ: ' + str(error)) from error


def verify_file(root, path, manifest):
    root, path = Path(root).resolve(), Path(path).resolve()
    if not path.is_relative_to(root):
        raise ValueError('File nằm ngoài serving export.')
    relative = path.relative_to(root).as_posix()
    expected = manifest['files'].get(relative)
    if expected is None or not path.is_file() or sha256(path) != expected:
        raise ValueError('Checksum local không khớp serving manifest: ' + relative)


def verify_bundle(root):
    manifest = read_manifest(root)
    for relative in manifest['files']:
        verify_file(root, Path(root) / relative, manifest)
    return manifest
