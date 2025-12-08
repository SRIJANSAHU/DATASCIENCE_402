"""
File Versioning System
"""

# imports
from pathlib import Path
import shutil
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

class VersionedFile:
    def __init__(self, filename: str):
        # Resolve path relative to current script
        base = Path(__file__).resolve().parent
        self.filepath = base / filename
        self.backup = self.filepath.with_suffix(self.filepath.suffix + ".bak")

    def __enter__(self):
        # 1. Create backup
        if self.filepath.exists():
            shutil.copy2(self.filepath, self.backup)
            logging.info(f"Backup created: {self.backup.name}")
        else:
            # If file doesn't exist, create an empty one
            self.filepath.touch()
            shutil.copy2(self.filepath, self.backup)
            logging.info(f"New file created and backed up: {self.backup.name}")

        # 2. Open file for writing (user edits this file)
        self._file = open(self.filepath, "w", encoding="utf-8")
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

        if exc_type is not None:
            # 3. Exception → restore backup
            shutil.copy2(self.backup, self.filepath)
            logging.error("Exception detected! File restored from backup.")
        else:
            logging.info("Changes saved successfully.")

        # Always allow exception to propagate if needed
        return False
    
# mainline execution

if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    # put the test file inside your existing structure:
    target_rel = Path("data") / "output" / "config.txt"
    target = base / target_rel

    # ensure the folder exists and seed initial content once
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text("original content\n", encoding="utf-8")
        print(f"Seeded: {target_rel}")

    print("\n1) Successful update...")
    with VersionedFile(str(target_rel)) as f:
        f.write("updated content!\n")

    print("   -> Done. Now testing rollback on exception...")
    try:
        with VersionedFile(str(target_rel)) as f:
            f.write("this will be rolled back\n")
            raise RuntimeError("Simulated failure")
    except RuntimeError as e:
        print(f"   -> Caught error: {e}. Rollback should have happened.")

    # show final file contents so you can verify
    final_text = target.read_text(encoding="utf-8").strip()
    print(f"\nFinal file content @ {target_rel}:\n{final_text}")
    print(f"Backup file @ {target.with_suffix(target.suffix + '.bak')}")
