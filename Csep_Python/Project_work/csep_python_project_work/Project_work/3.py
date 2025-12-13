"""News Sentiment Aggregator"""
# imports

import asyncio
import aiohttp
import json
import xml.etree.ElementTree as ET
from itertools import groupby
from pathlib import Path


# BASE PATH

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data/output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Simulated API Responses

FAKE_NEWS_DATA = {
    "https://news.api/tech": [
        {"title": "AI revolution in 2025", "content": "AI will change everything", "category": "tech"},
        {"title": "Python 4.0 Released", "content": "Faster and cleaner syntax", "category": "tech"},
    ],
    "https://news.api/sports": [
        {"title": "Team X wins championship", "content": "A historic win today!", "category": "sports"},
        {"title": "Player retires", "content": "An emotional farewell", "category": "sports"},
    ],
    "https://news.api/politics": [
        {"title": "Election updates", "content": "Major events unfolding", "category": "politics"},
        {"title": "New policy announced", "content": "Mixed reactions across states", "category": "politics"},
    ],
}

# Async API Fetcher

async def fetch_news(session, url: str):
    """Simulates an async API call using aiohttp."""
    await asyncio.sleep(0.5)  # simulate network delay
    return FAKE_NEWS_DATA.get(url, [])

# Sentiment Scoring Using FP (map/filter)

POSITIVE_WORDS = {"good", "excellent", "great", "win", "faster", "historic", "change"}
NEGATIVE_WORDS = {"bad", "slow", "terrible", "loss", "problem"}

def compute_sentiment(text: str) -> int:
    """Simple sentiment scoring using word matching."""
    words = text.lower().split()
    pos = len(list(filter(lambda w: w in POSITIVE_WORDS, words)))
    neg = len(list(filter(lambda w: w in NEGATIVE_WORDS, words)))
    return pos - neg  # positive if >0, negative if <0, neutral = 0

# XML Writer

def save_xml(data, file_path):
    root = ET.Element("news")

    for item in data:
        entry = ET.SubElement(root, "article")

        for key, value in item.items():
            elem = ET.SubElement(entry, key)
            elem.text = str(value)

    tree = ET.ElementTree(root)
    tree.write(file_path, encoding="utf-8", xml_declaration=True)

# Pipeline

async def process_news():
    urls = [
        "https://news.api/tech",
        "https://news.api/sports",
        "https://news.api/politics",
    ]

    async with aiohttp.ClientSession() as session:
        # Parallel fetch using asyncio.gather
        responses = await asyncio.gather(*(fetch_news(session, url) for url in urls))

    # Flatten list
    all_articles = [item for sublist in responses for item in sublist]

    # Add sentiment using FP
    processed = list(map(
        lambda article: {
            **article,
            "sentiment": compute_sentiment(article["title"] + " " + article["content"])
        },
        all_articles
    ))

    # Sort before groupby
    processed.sort(key=lambda x: x["category"])

    # Group by category
    grouped = {
        category: list(items)
        for category, items in groupby(processed, key=lambda x: x["category"])
    }

    # Save JSON
    json_path = OUTPUT_DIR / "news_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(grouped, f, indent=4)

    # Save XML
    xml_path = OUTPUT_DIR / "news_results.xml"
    xml_data = [item for sublist in grouped.values() for item in sublist]
    save_xml(xml_data, xml_path)

    return {
        "message": "News processed successfully",
        "json_saved_to": str(json_path),
        "xml_saved_to": str(xml_path),
        "categories": list(grouped.keys())
    }

# mainline execution

if __name__ == "__main__":
    result = asyncio.run(process_news())
    print(json.dumps(result, indent=4))
