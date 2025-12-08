"""
Large Text File 'LazyReader'
"""

# imports
from pathlib import Path
from collections import Counter
import json
import re
import logging

# ---- basic logging (optional but handy) ----
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ---- paths based on the screenshot, resolved via Path(__file__) ----
BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "log_dataset.txt"
OUTPUT_DIR = BASE_DIR / "data" / "output"
RESULTS_JSON = OUTPUT_DIR / "results_Large_file_analyzar.json"
SUMMARY_TXT = OUTPUT_DIR / "summary_Large_file_analyzer.txt"

# ---- Generator:  ----
def read_in_chunks(filepath: Path, chunk_size: int = 1 << 20):
    """
    Lazily read a large text file without loading it all in memory.
    Reads 'chunk_size' bytes at a time, but yields one line at a time.
    """
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        carry = ""
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                if carry:
                    yield carry
                break
            carry += chunk
            lines = carry.split("\n")
            for line in lines[:-1]:
                yield line
            carry = lines[-1]  # leftover partial line for the next read

# ---- Custom iterator: WordIterator ----
class WordIterator:
    """
    Iterate one word at a time from a text chunk.
    Words are [A-Za-z0-9']+, lowercased.
    """
    _word_re = re.compile(r"[A-Za-z0-9']+")

    def __init__(self, text_chunk: str):
        self._words = self._word_re.findall(text_chunk.lower())
        self._i = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if self._i >= len(self._words):
            raise StopIteration
        w = self._words[self._i]
        self._i += 1
        return w

# ---- Counting logic ----
def analyze_file(in_path: Path, chunk_size: int = 1 << 20):
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_words = 0
    freq = Counter()

    logging.info(f"Reading: {in_path} (chunk_size={chunk_size} bytes)")
    for line in read_in_chunks(in_path, chunk_size):
        for word in WordIterator(line):
            total_words += 1
            freq[word] += 1

    unique_words = len(freq)
    top10 = freq.most_common(10)

    # write JSON
    results = {
        "total_words": total_words,
        "unique_words": unique_words,
        "top_10": [{"word": w, "count": c} for w, c in top10],
    }
    RESULTS_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")

    # write summary
    lines = [
        f"Input file: {in_path.name}",
        f"Total words: {total_words}",
        f"Unique words: {unique_words}",
        "Top 10 words:",
    ]
    for w, c in top10:
        lines.append(f"  {w}: {c}")
    SUMMARY_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logging.info(f"Wrote: {RESULTS_JSON}")
    logging.info(f"Wrote: {SUMMARY_TXT}")

if __name__ == "__main__":
    try:
        analyze_file(INPUT_FILE, chunk_size=1 << 20)  # ~1MB chunks
    except Exception as e:
        logging.exception(e)
