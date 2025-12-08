"""
OOP-Based Log Analyzer
"""

# imports
import json
from pathlib import Path

class LogRecord:
    def __init__(self, ip, timestamp, method, path, status, size):
        self.ip = ip
        self.timestamp = timestamp  # keep as string
        self.method = method
        self.path = path
        self.status = status
        self.size = size  # int or None

    def to_dict(self):
        return {
            "ip": self.ip,
            "timestamp": self.timestamp,
            "method": self.method,
            "path": self.path,
            "status": self.status,
            "size": self.size,
        }

class LogParser:
    """
    Parsing is done with simple string operations.
    """

    def __init__(self, source_path: Path):
        self.source_path = source_path

    def _parse_line(self, line: str):
        # IP: first token
        ip = line.split(" ", 1)[0]

        # timestamp between '[' and ']'
        l_br = line.find("[")
        r_br = line.find("]", l_br + 1)
        timestamp = line[l_br + 1 : r_br] if (l_br != -1 and r_br != -1) else "-"

        # request between first and second double quotes
        q1 = line.find('"', r_br)
        q2 = line.find('"', q1 + 1) if q1 != -1 else -1
        request = line[q1 + 1 : q2] if (q1 != -1 and q2 != -1) else ""

        req_parts = request.split()
        method = req_parts[0] if len(req_parts) >= 1 else "-"
        path = req_parts[1] if len(req_parts) >= 2 else "-"

        # the rest after the closing quote: status size ...
        tail = line[q2 + 1 :].strip() if q2 != -1 else ""
        tail_parts = tail.split()
        status = int(tail_parts[0]) if len(tail_parts) >= 1 and tail_parts[0].isdigit() else 0

        size = None
        if len(tail_parts) >= 2:
            size = None if tail_parts[1] == "-" else int(tail_parts[1])

        return LogRecord(ip, timestamp, method, path, status, size)

    def parse(self):
        if not self.source_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.source_path}")
        records = []
        with self.source_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(self._parse_line(line))
                except Exception:
                    # skip badly-formatted lines silently (keep it simple)
                    pass
        return records

class LogAnalyzer:
    def __init__(self, records):
        self.records = list(records)

    def requests_per_ip(self):
        ips = list(map(lambda r: r.ip, self.records))  # map
        counts = {}
        for ip in ips:
            counts[ip] = counts.get(ip, 0) + 1
        return counts

    def status_counts(self):
        c200 = len(list(filter(lambda r: r.status == 200, self.records)))  # filter
        c404 = len(list(filter(lambda r: r.status == 404, self.records)))
        c403 = len(list(filter(lambda r: r.status == 403, self.records)))
        return {"200": c200, "404": c404, "403": c403}

    def total_data_transferred(self):
        return sum(map(lambda r: r.size or 0, self.records))  # map

    def results(self):
        return {
            "total_records": len(self.records),
            "requests_per_ip": self.requests_per_ip(),
            "status_counts": self.status_counts(),
            "total_data_transferred": self.total_data_transferred(),
        }

class Exporter:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def to_json(self, data, name="results.json"):
        p = self.out_dir / name
        with p.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return p

    def to_text(self, data, name="summary.txt"):
        p = self.out_dir / name
        lines = [
            "Log Analysis Summary",
            "--------------------",
            f"Total records: {data.get('total_records', 0)}",
            "",
            "Requests per IP:",
        ]
        rpi = data.get("requests_per_ip", {})
        for ip, cnt in sorted(rpi.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"  {ip:<15} -> {cnt}")

        sc = data.get("status_counts", {})
        lines += [
            "",
            "Status counts:",
            f"  200 : {sc.get('200', 0)}",
            f"  404 : {sc.get('404', 0)}",
            f"  403 : {sc.get('403', 0)}",
            "",
            f"Total data transferred (bytes): {data.get('total_data_transferred', 0)}",
            "",
        ]
        with p.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return p


def main():
    base = Path(__file__).resolve().parent
    input_file = base / "data" / "input" / "log_dataset.txt"
    output_dir = base / "data" / "output"

    try:
        records = LogParser(input_file).parse()
        results = LogAnalyzer(records).results()
        exp = Exporter(output_dir)
        json_path = exp.to_json(results)
        text_path = exp.to_text(results)
        print("Done.")
        print("JSON :", json_path)
        print("Text :", text_path)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
