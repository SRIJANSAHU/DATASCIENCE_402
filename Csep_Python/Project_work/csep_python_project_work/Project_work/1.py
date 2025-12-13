from __future__ import annotations

import json
from collections import Counter
from functools import reduce
from pathlib import Path
import threading

# ---------------- Core ----------------

class LogAnalyzer:
    """
    Simple log analyzer
    """

    def __init__(self, input_path: Path, output_path: Path, chunk_size: int = 50_000, threads: int = 4):
        self.input_path = input_path
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.threads = max(1, threads)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    # -------- Generators ----------
    def iter_lines(self):
        with self.input_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                yield line.rstrip("\n")

    def iter_chunks(self, lines):
        chunk = []
        for line in lines:
            chunk.append(line)
            if len(chunk) >= self.chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    # -------- Parsing ----------
    @staticmethod
    def parse_line(line: str):
        # Expect: "YYYY-mm-dd HH:MM:SS,ms LEVEL ..."
        parts = line.split(" ", 3)
        if len(parts) < 4:
            return None
        date, time_ms, level, rest = parts[0], parts[1], parts[2], parts[3]
        # normalize
        level = "WARNING" if level.startswith("WARN") else level
        if level not in ("INFO", "WARNING", "ERROR"):
            return None
        ts_second = f"{date} {time_ms.split(',', 1)[0]}"
        # message (after first ':')
        msg = rest.split(":", 1)[1].strip() if ":" in rest else rest.strip()
        return (level, ts_second, msg)

    # -------- Per-chunk aggregation ----------
    def aggregate_chunk(self, lines):
        parsed = filter(lambda x: x is not None, map(self.parse_line, lines))

        levels = Counter()
        rps = Counter()
        errors = Counter()
        parsed_cnt = 0

        for level, ts_second, msg in parsed:
            parsed_cnt += 1
            levels[level] += 1
            rps[ts_second] += 1
            if level == "ERROR":
                errors[msg] += 1

        return {
            "levels": levels,
            "rps": rps,
            "errors": errors,
            "total_lines": len(lines),
            "parsed_lines": parsed_cnt,
        }

    @staticmethod
    def merge(a, b):
        a["levels"].update(b["levels"])
        a["rps"].update(b["rps"])
        a["errors"].update(b["errors"])
        a["total_lines"] += b["total_lines"]
        a["parsed_lines"] += b["parsed_lines"]
        return a

    # -------- Threading : split chunk list among threads ----------
    def process(self):
        chunks = list(self.iter_chunks(self.iter_lines()))
        if not chunks:
            return {
                "file": str(self.input_path),
                "total_lines": 0,
                "parsed_lines": 0,
                "level_counts": {},
                "most_frequent_error": {"message": "", "count": 0},
                "rps": {"peak_second": "", "peak_value": 0, "distinct_seconds": 0},
            }

        # Divide chunks into N contiguous slices
        n = min(self.threads, len(chunks))
        size = (len(chunks) + n - 1) // n
        slices = [chunks[i * size : (i + 1) * size] for i in range(n)]

        partials = []
        lock = threading.Lock()

        def worker(subchunks):
            local_partials = list(map(self.aggregate_chunk, subchunks))
            # reduce within thread to minimize merging work
            if local_partials:
                base = {
                    "levels": Counter(),
                    "rps": Counter(),
                    "errors": Counter(),
                    "total_lines": 0,
                    "parsed_lines": 0,
                }
                combined = reduce(LogAnalyzer.merge, local_partials, base)
                with lock:
                    partials.append(combined)

        threads = [threading.Thread(target=worker, args=(slc,), daemon=True) for slc in slices]
        for t in threads: t.start()
        for t in threads: t.join()

        # Final reduce across thread outputs
        base = {
            "levels": Counter(),
            "rps": Counter(),
            "errors": Counter(),
            "total_lines": 0,
            "parsed_lines": 0,
        }
        final = reduce(LogAnalyzer.merge, partials, base)

        most_err_msg, most_err_cnt = ("", 0)
        if final["errors"]:
            most_err_msg, most_err_cnt = final["errors"].most_common(1)[0]

        peak_second, peak_val = ("", 0)
        if final["rps"]:
            peak_second, peak_val = final["rps"].most_common(1)[0]

        return {
            "file": str(self.input_path),
            "total_lines": final["total_lines"],
            "parsed_lines": final["parsed_lines"],
            "level_counts": dict(final["levels"]),
            "most_frequent_error": {"message": most_err_msg, "count": most_err_cnt},
            "rps": {
                "peak_second": peak_second,
                "peak_value": peak_val,
                "distinct_seconds": len(final["rps"]),
            },
        }

    def save_json(self, data: dict):
        self.output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

# mainline execution

def main():
    input_file = Path(__file__).parent.joinpath("data", "input", "Hadoop_2k.log")
    output_file = Path(__file__).parent.joinpath("data", "output", "log_results.json")

    analyzer = LogAnalyzer(input_file, output_file, chunk_size=20_000, threads=4)
    summary = analyzer.process()
    analyzer.save_json(summary)

    print(f"Processed: {summary['file']}")
    print(f"Total lines: {summary['total_lines']} | Parsed: {summary['parsed_lines']}")
    print(f"Levels: {summary['level_counts']}")
    print(f"Top ERROR: {summary['most_frequent_error']['message']!r} ({summary['most_frequent_error']['count']})")
    print(f"Peak RPS: {summary['rps']['peak_value']} at {summary['rps']['peak_second']}")
    print(f"Wrote JSON -> {output_file}")

if __name__ == "__main__":
    main()
