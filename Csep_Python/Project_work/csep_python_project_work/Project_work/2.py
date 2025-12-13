"""ETL Pipeline with FastAPI"""

from fastapi import FastAPI, UploadFile, File, HTTPException
import json
import csv
from pathlib import Path
import time
import traceback
from typing import List, Any

app = FastAPI()

# ------- PATH -------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ------- Context Manager — Log request time + errors -------
class RequestLogger:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        end = time.time()
        duration = round(end - self.start, 4)

        if exc_type:
            print("ERROR during request:", exc)
            traceback.print_tb(tb)
        print(f"Request finished in {duration} sec")


# ------- Decorator — Validate Uploaded Files -------

def validate_file(func):
    async def wrapper(file: UploadFile = File(...)):
        if not (file.filename.endswith(".csv") or file.filename.endswith(".json")):
            raise HTTPException(
                status_code=400,
                detail="Only CSV or JSON files are allowed."
            )
        return await func(file)
    return wrapper

# ------- ETL CLASSES -------

class DataCleaner:
    """Remove empty rows or invalid values"""

    @staticmethod
    def clean(data: List[dict]) -> List[dict]:
        return [row for row in data if all(val not in ("", None) for val in row.values())]

class DataTransformer:
    """Example transformation: Add a new column"""

    @staticmethod
    def transform(data: List[dict]) -> List[dict]:
        for row in data:
            row["record_length"] = len(row)
        return data

class ETLProcessor:
    """Main ETL handler using DataCleaner + DataTransformer"""

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def load_file(self) -> List[dict]:
        if self.file_path.suffix == ".csv":
            with open(self.file_path, newline="", encoding="utf-8") as f:
                return list(csv.DictReader(f))

        if self.file_path.suffix == ".json":
            with open(self.file_path, encoding="utf-8") as f:
                return json.load(f)

        raise ValueError("Unsupported file format")

    def process(self) -> List[dict]:
        data = self.load_file()
        data = DataCleaner.clean(data)
        data = DataTransformer.transform(data)
        return data

    def save_output(self, cleaned_data: List[dict], output_name: str):
        output_file = OUTPUT_DIR / output_name
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, indent=4)
        return output_file

# ------- FastAPI endpoints — Async handlers

@app.post("/upload")
@validate_file
async def upload_file(file: UploadFile = File(...)):
    with RequestLogger():

        # Save file to input directory
        file_path = INPUT_DIR / file.filename
        with open(file_path, "wb") as f:
            f.write(await file.read())

        # Run ETL
        etl = ETLProcessor(file_path)
        processed_data = etl.process()

        # Save output JSON
        output_name = file.filename.split(".")[0] + "_processed.json"
        saved_path = etl.save_output(processed_data, output_name)

        return {
            "message": "File processed successfully",
            "saved_to": str(saved_path),
            "records": len(processed_data)
        }

@app.get("/list-input-files")
async def list_input_files():
    files = [f.name for f in INPUT_DIR.glob("*")]
    return {"input_files": files}

@app.get("/list-output-files")
async def list_output_files():
    files = [f.name for f in OUTPUT_DIR.glob("*")]
    return {"output_files": files}

# ------- FastAPI run command -------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("2:app", reload=True)

# http://127.0.0.1:8000/docs 
# use customers.csv and products.json from /data/input