"""
High-Performance Recommendation Data Preprocessor
"""

# imports

import json
from pathlib import Path
from itertools import groupby, islice
from concurrent.futures import ProcessPoolExecutor

# --------------------- Utility Functions --------------------- #

def batched(iterable, n):
    """Yield items from iterable in chunks of size n."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch

def count_groupby(values):
    """Count occurrences using itertools.groupby (requires sorting)."""
    counts = {}
    for key, group in groupby(sorted(values)):
        counts[key] = sum(1 for _ in group)
    return counts

# --------------------- Preprocessor Class --------------------- #

class InteractionPreprocessor:

    def __init__(self):
        root = Path(__file__).parent
        self.input_file = root.joinpath("data", "input", "interactions.txt")
        self.output_dir = root.joinpath("data", "output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # -------- Generator Reading -------- #
    def iter_lines(self):
        with self.input_file.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                yield line.strip()

    def parse_interaction(self, line):
        parts = line.split(",")
        if len(parts) < 2:
            return None
        return parts[0].strip(), parts[1].strip()

    def iter_interactions(self, limit=None):
        lines = self.iter_lines()
        if limit:
            lines = islice(lines, limit)
        for line in lines:
            if not line:
                continue
            parsed = self.parse_interaction(line)
            if parsed:
                yield parsed

    # -------- Batch Worker (multiprocessing) -------- #
    @staticmethod
    def process_batch(batch):
        users = [u for u, _ in batch]
        items = [i for _, i in batch]

        user_counts = count_groupby(users)
        item_counts = count_groupby(items)

        return item_counts, user_counts

    # -------- Merge helper -------- #
    @staticmethod
    def merge_counts(a, b):
        for key, v in b.items():
            a[key] = a.get(key, 0) + v
        return a

    # -------- Full pipeline -------- #
    def run(self, batch_size=100000, limit=None, workers=None):
        item_global = {}
        user_global = {}

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = []
            for batch in batched(self.iter_interactions(limit), batch_size):
                futures.append(executor.submit(self.process_batch, batch))

            for fut in futures:
                item_part, user_part = fut.result()
                self.merge_counts(item_global, item_part)
                self.merge_counts(user_global, user_part)

        return item_global, user_global

    # -------- Save outputs -------- #
    def save(self, data, filename):
        path = self.output_dir.joinpath(filename)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f)
        print("Saved:", path)


# mainline execution

def main():
    pre = InteractionPreprocessor()
    item_counts, user_counts = pre.run(
        batch_size=50000,
        limit=None,
        workers=None
    )
    pre.save(item_counts, "item_popularity.json")
    pre.save(user_counts, "user_engagement.json")


if __name__ == "__main__":
    main()