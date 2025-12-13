from __future__ import annotations

import json
from pathlib import Path
import xml.etree.ElementTree as ET

# ----------------- Custom Context Manager -----------------
class SmartIO:
    def __init__(self, path: Path, mode: str = "r", encoding: str | None = "utf-8"):
        self.path, self.mode, self.encoding, self.fh = path, mode, encoding, None
    def __enter__(self):
        if any(m in self.mode for m in ("w", "a", "x", "+")):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.path, self.mode, encoding=self.encoding)  # noqa: PTH123
        return self.fh
    def __exit__(self, exc_type, exc, tb):
        if self.fh:
            self.fh.close()
        return False  # errors

# ----------------- OOP Readers -----------------
class FileReader:
    def read(self, path: Path) -> list[dict]:
        raise NotImplementedError

class JSONParser(FileReader):
    def read(self, path: Path) -> list[dict]:
        text = path.read_text(encoding="utf-8").strip()
        out: list[dict] = []
        if not text:
            return out
        # Try normal JSON (object/array)
        try:
            obj = json.loads(text)
            items = obj if isinstance(obj, list) else [obj]
            return [{"source_file": path.name, "source_format": "json", "payload": x} for x in items]
        except Exception:
            pass
        # Fallback: JSON Lines
        for i, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                out.append({"source_file": path.name, "source_format": "jsonl", "record_index": i, "payload": json.loads(line)})
            except Exception as e:
                out.append({"source_file": path.name, "source_format": "jsonl", "record_index": i, "error": f"json_parse_error:{e}", "payload": None})
        return out

class XMLParser(FileReader):
    def read(self, path: Path) -> list[dict]:
        try:
            root = ET.fromstring(path.read_text(encoding="utf-8"))
        except Exception as e:
            return [{"source_file": path.name, "source_format": "xml", "error": f"xml_parse_error:{e}", "payload": None}]
        def flat(e: ET.Element, p: str = "") -> dict:
            d: dict = {}
            if (e.text or "").strip():
                d[p + "#text"] = e.text.strip()
            for k, v in e.attrib.items():
                d[p + f"@{k}"] = v
            tag_counts = {}
            for c in e: tag_counts[c.tag] = tag_counts.get(c.tag, 0) + 1
            idx = {}
            for c in e:
                t = c.tag
                n = idx.get(t, 0); idx[t] = n + 1
                prefix = f"{p}{t}[{n}]." if tag_counts[t] > 1 else f"{p}{t}."
                d.update(flat(c, prefix))
            return d
        # If root has repeated children, emit one record per child; else single record
        children = list(root)
        repeated = any(sum(1 for c in children if c.tag == tag) > 1 for tag in {c.tag for c in children})
        if repeated:
            return [{"source_file": path.name, "source_format": "xml", "record_index": i, "payload": flat(c, f"{c.tag}.")} for i, c in enumerate(children)]
        return [{"source_file": path.name, "source_format": "xml", "payload": flat(root, f"{root.tag}.")}]

class TextReader(FileReader):
    def read(self, path: Path) -> list[dict]:
        recs: list[dict] = []
        with SmartIO(path, "r", "utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.rstrip("\n")
                if line.strip():
                    recs.append({"source_file": path.name, "source_format": "txt", "record_index": i, "payload": {"text": line}})
        return recs

# ----------------- Orchestrator -----------------
class MiniDataLake:
    def __init__(self, landing: Path, processed: Path):
        self.landing, self.processed = landing, processed
        self.processed.mkdir(parents=True, exist_ok=True)
        self.readers: dict[str, FileReader] = {
            ".json": JSONParser(),
            ".jsonl": JSONParser(),
            ".ndjson": JSONParser(),
            ".xml": XMLParser(),
            ".txt": TextReader(),
        }

    def discover(self) -> list[Path]:
        files: list[Path] = []
        for ext in self.readers.keys():
            files.extend(self.landing.glob(f"*{ext}"))
        return files

    def write_processed(self, src: Path, records: list[dict]) -> Path:
        out_path = self.processed / (src.stem + ".json")
        with SmartIO(out_path, "w", "utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        return out_path

    def run(self) -> None:
        for p in self.discover():
            reader = self.readers.get(p.suffix.lower())
            if not reader:
                continue
            out = reader.read(p)
            self.write_processed(p, out)

# mainline execution
def main():
    root = Path(__file__).resolve().parent
    landing = (Path(__file__).resolve().parent / "data" / "landing")
    processed = (Path(__file__).resolve().parent / "data" / "processed")

    # minimal seed so it runs if empty
    landing.mkdir(parents=True, exist_ok=True)
    if not any(landing.iterdir()):
        (landing / "sample.json").write_text(json.dumps({"hello": "world"}), encoding="utf-8")
        (landing / "sample.jsonl").write_text(json.dumps({"a": 1}) + "\n" + "{bad}\n", encoding="utf-8")
        (landing / "sample.xml").write_text("<root><item id='1'>x</item><item id='2'>y</item></root>", encoding="utf-8")
        (landing / "sample.txt").write_text("first line\n\nsecond line\n", encoding="utf-8")

    lake = MiniDataLake(landing, processed)
    lake.run()
    print("Done.")
    print("Landing  :", landing)
    print("Processed:", processed)

if __name__ == "__main__":
    main()
