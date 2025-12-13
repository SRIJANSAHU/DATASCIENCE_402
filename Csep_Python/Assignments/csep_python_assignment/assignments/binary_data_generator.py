from pathlib import Path
import os

def generate_large_binary_file(size_mb=100):
    base = Path(__file__).resolve().parent
    output_file = base / "data" / "input" / "large_sample.bin"

    # Create directories if missing
    output_file.parent.mkdir(parents=True, exist_ok=True)

    size_bytes = size_mb * 1024 * 1024  # Convert MB → bytes

    print(f"Generating {size_mb} MB binary file...")
    with output_file.open("wb") as f:
        f.write(os.urandom(size_bytes))  # Write random bytes

    print(f"✔ large_sample.bin created at: {output_file}")

if __name__ == "__main__":
    generate_large_binary_file(100)  # change to 200, 500 if needed
