"""Check required CSV files and headers; does not audit full data quality."""
import argparse
import csv
from pathlib import Path

SCHEMAS = {
    "aisles.csv": ["aisle_id", "aisle"],
    "departments.csv": ["department_id", "department"],
    "products.csv": ["product_id", "product_name", "aisle_id", "department_id"],
    "orders.csv": ["order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"],
    "order_products__prior.csv": ["order_id", "product_id", "add_to_cart_order", "reordered"],
    "order_products__train.csv": ["order_id", "product_id", "add_to_cart_order", "reordered"],
}

def check(directory):
    failures = 0
    for name, expected in SCHEMAS.items():
        path = directory / name
        try:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                header = next(csv.reader(stream), [])
            missing = set(expected) - set(header)
            if missing or len(header) != len(set(header)):
                print(f"FAIL {name}: missing={sorted(missing)}, duplicate_headers={len(header) != len(set(header))}")
                failures += 1
            else:
                print(f"PASS {name}: {path.stat().st_size:,} bytes; required headers present")
        except (OSError, UnicodeError, csv.Error) as error:
            print(f"FAIL {name}: {error}")
            failures += 1
    print("Scope: file/header check only; no row counts or data-quality audit.")
    return 1 if failures else 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[1] / "data" / "raw")
    raise SystemExit(check(parser.parse_args().data_dir))
