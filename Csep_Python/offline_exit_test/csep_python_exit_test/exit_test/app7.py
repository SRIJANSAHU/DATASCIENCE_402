# imports
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import logging, re

app = FastAPI(title="Mini Text Analytics")
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

WORD_RE = re.compile(r"[A-Za-z0-9']+")

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file uploaded.")
        raw = await file.read()
        text = raw.decode("utf-8", errors="ignore")
        words = WORD_RE.findall(text.lower())

        word_count = len(words)
        unique_words = len(set(words))
        longest_word = max(words, key=len) if words else None
        shortest_word = min(words, key=len) if words else None

        result = {
            "filename": file.filename,
            "word_count": word_count,
            "unique_words": unique_words,
            "longest_word": longest_word,
            "shortest_word": shortest_word,
        }
        return JSONResponse(result)
    except HTTPException:
        raise
    except Exception as e:
        logging.exception(e)
        raise HTTPException(status_code=500, detail="Failed to analyze file.")