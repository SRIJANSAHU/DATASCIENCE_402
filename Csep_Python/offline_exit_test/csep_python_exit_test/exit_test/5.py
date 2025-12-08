"""
Parallel Processing Job: Text Corpus Analytics
- ThreadPoolExecutor -> read files in parallel (I/O bound)
- ProcessPoolExecutor -> heavy text stats per file (CPU bound)
- Merge all results into a single dictionary
"""

from __future__ import annotations
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import Counter
import logging
import json
import re
import os
import sys
from typing import Dict, List, Tuple

# ---------------------- basic logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)

# ---------------------- paths ----------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input" / "corpus"       # preferred place for many .txt files
FALLBACK_INPUT_DIR = DATA_DIR / "input"         # fallback: scan plain input dir
OUTPUT_DIR = DATA_DIR / "output"
RESULTS_JSON = OUTPUT_DIR / "results_corpus.json"
SUMMARY_TXT = OUTPUT_DIR / "summary_corpus.txt"

WORD_RE = re.compile(r"[A-Za-z0-9']+")

# ---------------------- helpers ----------------------
def seed_corpus_if_missing(target: Path, n_files: int = 10) -> None:
    """Create a tiny demo corpus if nothing exists so the script runs out-of-the-box."""
    if target.exists() and any(target.glob("*.txt")):
        return
    target.mkdir(parents=True, exist_ok=True)
    sample = (
        "This is a small demo document. It has repeated words words words.\n"
        "Lexical diversity tests uniqueness over total word count.\n"
    )
    for i in range(1, n_files + 1):
        (target / f"doc_{i:02d}.txt").write_text(sample * i, encoding="utf-8")
    logging.info(f"Seeded {n_files} demo files at: {target}")

def discover_corpus() -> List[Path]:
    """Find .txt files (>=10 recommended) under input/corpus or fallback to input."""
    if INPUT_DIR.exists():
        files = sorted(INPUT_DIR.glob("*.txt"))
        if files:
            return files
    if FALLBACK_INPUT_DIR.exists():
        files = sorted((p for p in FALLBACK_INPUT_DIR.glob("*.txt")))
        if files:
            return files
    # If nothing found, seed and return
    seed_corpus_if_missing(INPUT_DIR, n_files=10)
    return sorted(INPUT_DIR.glob("*.txt"))

def read_file_text(path: Path) -> Tuple[Path, str]:
    """Read a single file (used by threads)."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        return path, text
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return path, ""

def read_files_in_parallel(paths: List[Path], max_workers: int | None = None) -> Dict[Path, str]:
    """Use threads to read many files concurrently (I/O bound)."""
    results: Dict[Path, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(read_file_text, p): p for p in paths}
        for fut in as_completed(futures):
            p, text = fut.result()
            results[p] = text
    return results

def heavy_stats(text: str) -> Dict[str, float | int]:
    """
    CPU-bound text analytics (used by processes):
    - word_count
    - char_count
    - unique_words
    - lexical_diversity = unique_words / word_count (0 if no words)
    """
    words = WORD_RE.findall(text.lower())
    word_count = len(words)
    unique_words = len(set(words))
    char_count = len(text)
    diversity = (unique_words / word_count) if word_count else 0.0
    return {
        "word_count": word_count,
        "char_count": char_count,
        "unique_words": unique_words,
        "lexical_diversity": diversity,
    }

def process_in_parallel(texts: Dict[Path, str], max_workers: int | None = None) -> Dict[Path, Dict]:
    """Use processes to compute heavy stats per file."""
    paths = list(texts.keys())
    blobs = [texts[p] for p in paths]
    results: Dict[Path, Dict] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for p, stats in zip(paths, pool.map(heavy_stats, blobs, chunksize=max(1, len(blobs)//(os.cpu_count() or 2)))):
            results[p] = stats
    return results

def merge_results(per_file: Dict[Path, Dict]) -> Dict:
    """Aggregate per-file stats into a single dictionary with totals and averages."""
    total_words = sum(s["word_count"] for s in per_file.values())
    total_chars = sum(s["char_count"] for s in per_file.values())
    total_unique_words = sum(s["unique_words"] for s in per_file.values())  # not global unique
    # Weighted average lexical diversity by word_count
    weighted_diversity = (
        sum(s["lexical_diversity"] * s["word_count"] for s in per_file.values()) / total_words
        if total_words else 0.0
    )

    merged = {
        "files_analyzed": len(per_file),
        "totals": {
            "words": total_words,
            "chars": total_chars,
            "unique_words_sum": total_unique_words,  # note: sum across files, not global unique
            "lexical_diversity_weighted": round(weighted_diversity, 6),
        },
        "per_file": {
            str(p.name): per_file[p] for p in sorted(per_file.keys())
        }
    }
    return merged

def write_outputs(result: Dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")

    lines = [
        f"Files analyzed: {result['files_analyzed']}",
        f"Total words: {result['totals']['words']}",
        f"Total chars: {result['totals']['chars']}",
        f"Sum of unique words (per-file): {result['totals']['unique_words_sum']}",
        f"Weighted lexical diversity: {result['totals']['lexical_diversity_weighted']}",
        "",
        "Per-file (word_count, unique_words, diversity):",
    ]
    for fname, s in result["per_file"].items():
        lines.append(f"  {fname}: words={s['word_count']}, unique={s['unique_words']}, diversity={round(s['lexical_diversity'],6)}")
    SUMMARY_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

# ---------------------- main ----------------------
def main():
    files = discover_corpus()
    if len(files) < 10:
        logging.warning(f"Found only {len(files)} files; seeding demo to reach 10.")
        seed_corpus_if_missing(INPUT_DIR, n_files=10)
        files = discover_corpus()

    logging.info(f"Reading {len(files)} files from: {files[0].parent}")

    texts = read_files_in_parallel(files)
    stats_per_file = process_in_parallel(texts)
    merged = merge_results(stats_per_file)
    write_outputs(merged)

    logging.info(f"Wrote: {RESULTS_JSON}")
    logging.info(f"Wrote: {SUMMARY_TXT}")

if __name__ == "__main__":
    # Windows safety for multiprocessing
    if sys.platform.startswith("win"):
        import multiprocessing as mp
        mp.freeze_support()
    main()
