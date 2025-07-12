"""
FINRA Reg-SHO daily short-sale volume scraper
============================================

• Downloads all Consolidated-NMS files (CNMS) from 2020-01-01 to today.
• Handles both plain-text (*.txt) and gzip-compressed (*.txt.gz) days.
• Skips holidays / missing files gracefully.
• Returns one DataFrame with columns:
      ['date', 'symbol', 'short_volume', 'total_volume', 'short_exempt_volume']
• Saves a local CSV snapshot so you don’t hit FINRA on every re-run.
"""

import datetime as dt
import gzip
import io
import os
import time
import requests
import pandas as pd

# ---------- 1. CONFIG --------------------------------------------------------
START_DATE = dt.date(2025, 7, 7)
END_DATE = dt.date.today()
MARKET_CODE = "CNMS"
OUTPUT_CSV = f"{MARKET_CODE}_shortvol_2020to{END_DATE}.csv"
RATE_LIMIT = 0.35                                           # seconds between requests
USER_AGENT = "short-sale-scraper/0.1 (+github.com/anusheel-sharma"

# ----------- 2. DOWNLOAD ONE DAY ----------------------------------------------
def fetch_one(date: dt.date) -> bytes | None:
    """
    Try both *.txt and *.txt.gz; return raw *plain-text* bytes, or None if 404/denied.
    """
    ymd = date.strftime("%Y%m%d")
    base = f"https://cdn.finra.org/equity/regsho/daily/{MARKET_CODE}shvol{ymd}"
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    for suffix, decompress in ((".txt", False), ('.txt.gz', True)):
        url = base + suffix
        r = session.get(url, timeout=20)
        if r.status_code == 200 and b"Access Denied" not in r.content:
            raw = gzip.decompress(r.content) if decompress else r.content
            return raw
    return None

# ----------- 3. MAIN LOOP -----------------------------------------------------
all_frames = []
current = START_DATE
one_day = dt.timedelta(days=1)

while current <= END_DATE:
    raw = fetch_one(current)
    if raw:
        buf = io.BytesIO(raw)
        df = pd.read_csv(buf, sep='|').rename(columns=str.lower)

        df["date"] = current
        all_frames.append(df)
        print(f"{current} - {len(df):5d} rows")
        time.sleep(RATE_LIMIT)
    else:
        # Holiday or missing file
        print(f"{current} - No file")
    
    current += one_day
      
# ---------- 4. CONCAT & SAVE -------------------------------------------------
if not all_frames:
    raise RuntimeError("No data downloaded – check internet / date range.")
master = pd.concat(all_frames, ignore_index=True)
master.drop('market', axis=1, inplace=True)     # Drop market/exchange column which is not required
master.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved {len(master):,} rows ➜ {OUTPUT_CSV}")
