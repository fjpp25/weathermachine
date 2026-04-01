# The Weather Machine

A momentum + forecast-informed trading system designed for the daily Kalshi city temperature markets.

## Strategy overview

1. **City profiles** (static, cached) — 30-year NOAA climate normals per city/month
2. **NWS live feed** (polled hourly) — current observations + today's forecast high
3. **Kalshi scanner** (polled every few minutes) — live bracket prices
4. **Decision engine** — compares forecast-implied probabilities to market prices

## Station map

| City | ICAO | Settlement note |
|---|---|---|
| New York | KNYC | Central Park — NOT JFK or LGA |
| Chicago | KMDW | Midway Airport — NOT O'Hare |
| Miami | KMIA | Miami International |
| Austin | KAUS | Bergstrom Airport |
| Los Angeles | KLAX | LAX Airport |
| San Francisco | KSFO | SFO Airport |
| Denver | KDEN | Denver International |
| Philadelphia | KPHL | Philadelphia International |

> ⚠️ Using the wrong station can cause 1–2°F errors that push you into the wrong bracket.

## Key resolution facts

- Kalshi settles on the **NWS CLI (Climatological Report)**, published the morning after
- CLI uses **raw sensor values**, not the F→C→F rounded values shown in real-time feeds
- Reporting period is **midnight to midnight Local Standard Time** (ignores DST)
- Settlement typically happens **6–9 AM ET** the following morning

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Step 1: Fetch and cache city profiles (run once)
python city_profiles.py
python city_profiles.py --show     # inspect the data

# Step 2: Live feed (coming soon)
# python nws_feed.py

# Step 3: Kalshi scanner (coming soon)
# python kalshi_scanner.py
```

## Project structure

```
kalshi_weather/
├── city_profiles.py       # Step 1: NOAA 30yr normals
├── nws_feed.py            # Step 2: Live observations + forecast (TODO)
├── kalshi_scanner.py      # Step 3: Live market prices (TODO)
├── decision_engine.py     # Step 4: Signal generation (TODO)
├── requirements.txt
└── data/
    └── city_profiles.json # Cached after first run
```
