"""
Weather Data Fetcher (requests + JSON + FP: map/filter/reduce)

- Fetch hourly temperature for 5–10 cities from Open-Meteo
- map: apply API calls to all cities
- filter: keep only successful responses
- reduce: compute overall average temperature across all hours & cities
- Save a human summary + raw JSON results
"""

from __future__ import annotations
from pathlib import Path
from functools import reduce
from typing import Dict, List, Tuple, Any
import requests
import json
import logging

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ---------- paths ----------
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "data" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_TXT = OUT_DIR / "weather_summary.txt"
RESULTS_JSON = OUT_DIR / "results_weather.json"

# ---------- dataset: 5–10 cities (lat, lon) ----------
CITIES: List[Tuple[str, float, float]] = [
    ("Berlin",        52.52,  13.41),
    ("London",        51.50,  -0.12),
    ("New York",      40.71, -74.01),
    ("San Francisco", 37.77, -122.42),
    ("Tokyo",         35.68, 139.69),
    ("Sydney",       -33.87, 151.21),
    ("Delhi",         28.61,  77.21),
    ("São Paulo",    -23.55, -46.63),
    ("Cairo",         30.04,  31.24),
    ("Johannesburg", -26.20,  28.04),
]

API_URL = "https://api.open-meteo.com/v1/forecast"  # fixed (user link had a line break)
PARAMS_BASE = {
    "hourly": "temperature_2m",
    # Optionally restrict to today; leaving default gives next 7 days hourly
    # "timezone": "auto",
}

# ---------- network ----------
def fetch_city_weather(city: Tuple[str, float, float], timeout: float = 10.0) -> Dict[str, Any] | None:
    """Fetch weather JSON for one city. Returns dict with 'city' and data, or None on failure."""
    name, lat, lon = city
    try:
        params = dict(PARAMS_BASE, latitude=lat, longitude=lon)
        resp = requests.get(API_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        return {"city": name, "data": data}
    except Exception as e:
        logging.warning(f"Failed for {name}: {e}")
        return None

# ---------- analytics ----------
def city_hourly_avg(entry: Dict[str, Any]) -> Tuple[str, float, int]:
    """
    Return (city_name, avg_temp, n_points)
    avg over all available hourly temperatures in the response.
    """
    name = entry["city"]
    hourly = entry["data"].get("hourly", {})
    temps = hourly.get("temperature_2m", []) or []
    if not temps:
        return name, float("nan"), 0
    avg = sum(temps) / len(temps)
    return name, avg, len(temps)

def reduce_overall_average(acc: Tuple[float, int], entry: Tuple[str, float, int]) -> Tuple[float, int]:
    """
    Accumulator for overall average across cities:
    acc -> (sum_of_all_temps, count_of_all_temps)
    entry -> (city, city_avg, city_count)  -> we need raw temps count, so add (avg * count).
    """
    total_sum, total_n = acc
    _, avg, n = entry
    if n == 0 or avg != avg:  # check NaN
        return total_sum, total_n
    return total_sum + avg * n, total_n + n

# ---------- main ----------
def main() -> None:
    # map -> perform API calls
    mapped = map(fetch_city_weather, CITIES)

    # filter -> remove failed calls
    ok_entries = list(filter(lambda x: x is not None, mapped))  # type: ignore

    if not ok_entries:
        raise SystemExit("No successful responses. Check internet or API availability.")

    # per-city averages
    per_city = list(map(city_hourly_avg, ok_entries))

    # reduce -> overall average across all hours & cities
    total_sum, total_n = reduce(reduce_overall_average, per_city, (0.0, 0))
    overall_avg = (total_sum / total_n) if total_n else float("nan")

    # persist JSON
    out = {
        "overall_avg_temperature": overall_avg,
        "total_points": total_n,
        "cities": [
            {"city": c, "avg_temp": avg, "points": n} for (c, avg, n) in per_city
        ],
    }
    RESULTS_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")

    # persist human summary
    lines = [
        f"Successful cities: {len(per_city)} / {len(CITIES)}",
        f"Overall hourly temperature average: {round(overall_avg, 3)} °C (across {total_n} points)",
        "",
        "Per-city averages:",
    ]
    for c, avg, n in per_city:
        lines.append(f"  - {c:14s} avg={round(avg,3):7} °C  points={n}")
    SUMMARY_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logging.info(f"Wrote: {RESULTS_JSON}")
    logging.info(f"Wrote: {SUMMARY_TXT}")

if __name__ == "__main__":
    main()
