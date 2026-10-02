"""One-time projection of the small product dimension into dashboard Gold."""
from pathlib import Path
import json
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]

def main():
    source = ROOT / 'data/silver/products'
    target = ROOT / 'data/gold/product_catalog'
    table = pq.read_table(source, columns=['product_id', 'product_name', 'department'])
    assert table.num_rows <= 100_000, 'Expected a small product dimension'
    ids = table.column('product_id').to_pylist()
    assert len(set(ids)) == len(ids) and None not in ids
    if target.exists():
        existing = pq.read_table(target)
        assert existing.equals(table), 'Existing catalog differs; refusing to overwrite'
    else:
        target.mkdir()
        pq.write_table(table, target / 'part-00000.parquet', compression='snappy')
    print(f'PASS: product_catalog={table.num_rows:,} rows')
    settings = yaml.safe_load((ROOT/'config/settings.yaml').read_text())
    (ROOT/'outputs/metrics/dashboard_manifest.json').write_text(json.dumps({
        'spark_project_config': settings['spark'],
        'config_source': 'config/settings.yaml',
        'product_catalog_rows': table.num_rows}, indent=2)+'\n')

if __name__ == '__main__':
    main()
