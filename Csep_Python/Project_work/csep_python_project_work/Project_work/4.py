from __future__ import annotations

import json
import logging
from functools import wraps
from multiprocessing import Pool, cpu_count
from pathlib import Path
from time import perf_counter
from itertools import islice
from typing import Dict, Iterator, List


# ----------------------------- Decorators -----------------------------

def timed(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        t0 = perf_counter()
        result = fn(*args, **kwargs)
        dt = perf_counter() - t0
        logging.info("TIMED: %s ran in %.3fs", fn.__name__, dt)
        return result
    return wrapper

def log_calls(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        logging.debug("CALL: %s", fn.__name__)
        return fn(*args, **kwargs)
    return wrapper

# ----------------------------- Generators -----------------------------

def stream_file(path: Path) -> Iterator[Dict]:
    """
    Streams input sensor file line-by-line.
    File format must be JSON Lines:
      {"device_id": "...", "temperature": 22.5, ...}
    """
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except:
                yield {"_corrupt": True, "raw": line}


def chunked(it: Iterator, n: int):
    """Yield lists of size n."""
    it = iter(it)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

# ----------------------------- Validation -----------------------------

def _is_missing(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip().lower() in {"", "nan", "null"})


def validate_single(rec: Dict):
    """
    Pure validation function.
    Returns: (record, is_clean, reason)
    """
    if rec.get("_corrupt"):
        return rec, False, "corrupt_json"

    required = ["device_id", "ts", "temperature", "humidity", "battery", "status"]
    missing = [f for f in required if _is_missing(rec.get(f))]
    if missing:
        return rec, False, f"missing:{','.join(missing)}"

    # Type checks
    if any(isinstance(rec[f], str) for f in ["temperature", "humidity", "battery"]):
        return rec, False, "invalid_type"

    # Range checks
    t, h, b = rec["temperature"], rec["humidity"], rec["battery"]
    if not (-40 <= t <= 85):
        return rec, False, "temperature_range"
    if not (0 <= h <= 100):
        return rec, False, "humidity_range"
    if not (0 <= b <= 100):
        return rec, False, "battery_range"

    if rec["status"] not in {"OK", "WARN", "FAIL"}:
        return rec, False, "status_invalid"

    return rec, True, ""

# ----------------------------- OOP Tool -------------------------------

class SensorDQTool:
    def __init__(self, input_file: Path, out_clean: Path, out_dirty: Path,
                 batch_size: int = 500, workers: int | None = None):
        self.input_file = input_file
        self.out_clean = out_clean
        self.out_dirty = out_dirty
        self.batch_size = batch_size
        self.workers = workers or max(1, cpu_count() - 1)

        self.out_clean.parent.mkdir(parents=True, exist_ok=True)
        self.out_dirty.parent.mkdir(parents=True, exist_ok=True)

    @log_calls
    @timed
    def process(self):
        clean, dirty = [], []

        stream = stream_file(self.input_file)

        with Pool(self.workers) as pool:
            for batch in chunked(stream, self.batch_size):
                results = pool.map(validate_single, batch)
                for rec, ok, reason in results:
                    if ok:
                        clean.append(rec)
                    else:
                        rec2 = dict(rec)
                        rec2["_reason"] = reason
                        dirty.append(rec2)

        # Write outputs
        self.out_clean.write_text(json.dumps(clean, indent=2), encoding="utf-8")
        self.out_dirty.write_text(json.dumps(dirty, indent=2), encoding="utf-8")

        logging.info("CLEAN=%s  DIRTY=%s", len(clean), len(dirty))
        return len(clean), len(dirty)

# mainline execution

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    root = Path(__file__).parent

    input_file = root.joinpath("data", "input", "sensor_data.jsonl")
    out_clean = root.joinpath("data", "output", "clean_records.json")
    out_dirty = root.joinpath("data", "output", "dirty_records.json")

    if not input_file.exists():
        raise FileNotFoundError(
            f"Place your sensor stream file at:\n{input_file}\n"
            "Format: one JSON object per line."
        )

    tool = SensorDQTool(input_file, out_clean, out_dirty, batch_size=1000)
    clean, dirty = tool.process()

    print("\nDONE")
    print("Clean Records :", clean)
    print("Dirty Records :", dirty)
    print("Clean Output  :", out_clean)
    print("Dirty Output  :", out_dirty)


if __name__ == "__main__":
    main()
