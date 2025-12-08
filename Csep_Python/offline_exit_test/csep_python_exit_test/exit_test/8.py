"""
End-to-End Retail Analytics Engine
"""

from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
from functools import reduce, wraps
from typing import List, Dict, Iterable, Callable, Tuple
import time
import logging

# ----------------- logging -----------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ----------------- paths -----------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
INPUT_FILE = INPUT_DIR / "retail.txt"
SUMMARY_FILE = OUTPUT_DIR / "retail_summary.txt"

# Decorators
def measure_time(func: Callable):
    """Decorator to time a function and log the elapsed time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            dt = (time.perf_counter() - t0) * 1000.0
            logging.info(f"{func.__name__} took {dt:.2f} ms")
    return wrapper

def require_fields(required: List[str]):
    """Decorator to ensure dict-like input has required fields."""
    def deco(func: Callable):
        @wraps(func)
        def wrapper(row: Dict[str, str], *args, **kwargs):
            missing = [k for k in required if k not in row or row[k] == "" or row[k] is None]
            if missing:
                raise ValueError(f"Missing required fields: {missing}")
            return func(row, *args, **kwargs)
        return wrapper
    return deco

# OOP Model
@dataclass(frozen=True)
class RetailRecord:
    order_id: int
    product: str
    category: str
    quantity: int
    price: float
    country: str

    @property
    def revenue(self) -> float:
        return self.quantity * self.price

class RetailDataset:
    def __init__(self, records: List[RetailRecord] | None = None):
        self.records: List[RetailRecord] = records or []

    def add(self, rec: RetailRecord) -> None:
        self.records.append(rec)

    def __iter__(self):
        return iter(self.records)

    def __len__(self):
        return len(self.records)

# File Loader
def _split_csv_like(line: str) -> List[str]:
    """
    CSV split by comma.
    Assumes no embedded commas/quotes. Strips whitespace.
    """
    return [part.strip() for part in line.strip().split(",")]

@require_fields(["order_id", "product", "category", "quantity", "price", "country"])
def _row_to_record(row: Dict[str, str]) -> RetailRecord:
    try:
        return RetailRecord(
            order_id=int(row["order_id"]),
            product=row["product"],
            category=row["category"],
            quantity=int(row["quantity"]),
            price=float(row["price"]),
            country=row["country"],
        )
    except Exception as e:
        raise ValueError(f"Bad row values: {row}") from e

@measure_time
def load_retail_file(path: Path) -> RetailDataset:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln for ln in f if ln.strip()]

    if not lines:
        return RetailDataset([])

    headers = _split_csv_like(lines[0])
    ds = RetailDataset()
    for ln in lines[1:]:
        cols = _split_csv_like(ln)
        if len(cols) != len(headers):
            logging.warning(f"Skipping malformed line: {ln!r}")
            continue
        row = dict(zip(headers, cols))
        try:
            rec = _row_to_record(row)
            ds.add(rec)
        except Exception as e:
            logging.warning(f"Skipping row due to error: {e}")
    return ds

# Functional Processing
def total_revenue(records: Iterable[RetailRecord]) -> float:
    # map -> revenue, reduce -> sum
    return reduce(lambda acc, r: acc + r.revenue, records, 0.0)

def total_quantity(records: Iterable[RetailRecord]) -> int:
    return reduce(lambda acc, r: acc + r.quantity, records, 0)

def revenue_per_category(records: Iterable[RetailRecord]) -> Dict[str, float]:
    def reducer(acc: Dict[str, float], r: RetailRecord) -> Dict[str, float]:
        acc[r.category] = acc.get(r.category, 0.0) + r.revenue
        return acc
    return reduce(reducer, records, {})

def top_n_categories_by_revenue(rev_by_cat: Dict[str, float], n: int = 3) -> List[Tuple[str, float]]:
    return sorted(rev_by_cat.items(), key=lambda kv: kv[1], reverse=True)[:n]

# Generators
def records_for_country(records: Iterable[RetailRecord], country: str):
    """Generator: yield records for a specific country."""
    for r in records:
        if r.country == country:
            yield r

def countries(records: Iterable[RetailRecord]):
    """Generator: yield unique countries encountered."""
    seen = set()
    for r in records:
        if r.country not in seen:
            seen.add(r.country)
            yield r.country

# Utilities

def write_summary(
    ds: RetailDataset,
    total_rev: float,
    total_qty: int,
    rev_by_cat: Dict[str, float],
    top3: List[Tuple[str, float]],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        f"Records loaded: {len(ds)}",
        f"Total revenue: {round(total_rev, 2)}",
        f"Total quantity sold: {total_qty}",
        "",
        "Revenue per category:",
    ]
    for cat, rev in sorted(rev_by_cat.items(), key=lambda kv: kv[0]):
        lines.append(f"  - {cat}: {round(rev, 2)}")
    lines.append("")
    lines.append("Top 3 categories by revenue:")
    for cat, rev in top3:
        lines.append(f"  - {cat}: {round(rev, 2)}")

    # Example usage of country generator
    lines.append("")
    lines.append("Countries present:")
    lines.extend(f"  - {c}" for c in countries(ds))

    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info(f"Wrote summary: {SUMMARY_FILE}")

# Main
def main():
    ds = load_retail_file(INPUT_FILE)

    # filter example: only positive quantities
    filtered_records = list(filter(lambda r: r.quantity > 0, ds))

    total_rev = total_revenue(filtered_records)
    total_qty = total_quantity(filtered_records)
    rev_by_cat = revenue_per_category(filtered_records)
    top3 = top_n_categories_by_revenue(rev_by_cat, n=3)

    write_summary(ds, total_rev, total_qty, rev_by_cat, top3)

    # quick console print
    print(f"Loaded {len(ds)} records")
    print(f"Total revenue: {round(total_rev, 2)}")
    print(f"Total quantity: {total_qty}")
    print("Top 3 categories:", top3)

    # demo: generator per country
    print("\nDemo (records for country = 'USA'):")
    for r in records_for_country(ds, "USA"):
        print(f"  order {r.order_id}: {r.product} x{r.quantity} -> ${r.revenue}")

if __name__ == "__main__":
    main()
