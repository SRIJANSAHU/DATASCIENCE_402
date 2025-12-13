import os
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

# ---------------- Generators ----------------

def iter_text_files(folder, suffix=".txt"):
    for p in Path(folder).rglob(f"*{suffix}"):
        if p.is_file():
            yield p

def iter_file_lines(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            yield i, line.rstrip("\n")

# ---------------- OOP: IndexBuilder ----------------

class IndexBuilder:
    def __init__(self, folder, suffix=".txt"):
        self.folder = Path(folder)
        self.suffix = suffix
        self._paths = []

    def build(self):
        self._paths = list(iter_text_files(self.folder, self.suffix))
        print(f"Indexed {len(self._paths)} text files from {self.folder}")
        return self._paths

    @property
    def paths(self):
        return self._paths

# ---------------- OOP: SearchEngine ----------------

class SearchEngine:
    def __init__(self, file_paths, case_insensitive=True):
        self.file_paths = list(file_paths)
        self.case_insensitive = case_insensitive

    def _norm(self, s):
        return s.casefold() if self.case_insensitive else s

    def _search_file(self, path, keywords, mode="OR"):
        norm_keywords = [self._norm(k) for k in keywords if k]
        hits = []

        def line_matches(text):
            t = self._norm(text)
            matched = list(filter(lambda k: k in t, norm_keywords))
            if mode == "AND":
                return (len(matched) == len(norm_keywords), matched)
            return (len(matched) > 0, matched)

        for ln, txt in iter_file_lines(path):
            ok, matched = line_matches(txt)
            if ok:
                hits.append({
                    "line_no": ln,
                    "line_text": txt,
                    "matched_keywords": matched
                })

        return (str(path), hits)

    def search(self, keywords, mode="OR"):
        results = {}
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as pool:
            for fp, hits in pool.map(lambda p: self._search_file(p, keywords, mode), self.file_paths):
                if hits:
                    results[fp] = hits
        return results

    @staticmethod
    def save_json(results, out_path):
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nSaved output to: {out_path}\n")

# mainline execution

# Read text files
docs_dir = Path(__file__).parent.joinpath("data","input")
saving_dir = Path(__file__).parent.joinpath("data","output","results.json")

index = IndexBuilder(docs_dir)
files = index.build()

# search engine
engine = SearchEngine(files)

# Example keywords
results_or = engine.search(["process", "intelligence"], mode="OR")
results_and = engine.search(["thought", "moment"], mode="AND")

# Merge results using itertools
merged = {}
for d in (results_or, results_and):
    for fp, hits in d.items():
        merged.setdefault(fp, [])
        merged[fp] = list(chain(merged[fp], hits))

# Save to JSON
engine.save_json(merged, saving_dir)

# Show summary
total = sum(len(v) for v in merged.values())
print(f"Total hits found: {total}")

if merged:
    one_file = next(iter(merged))
    print("Example hit:", merged[one_file][0])
