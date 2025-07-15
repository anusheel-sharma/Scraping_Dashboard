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
from pathlib import Path

# ---------- 1. CONFIG --------------------------------------------------------
START_DATE = dt.date(2019, 1, 1)
END_DATE = dt.date(2025, 7, 7)
MARKET_CODE = "CNMS"
OUTPUT = f"data/raw/{MARKET_CODE}_shortvol_2020to{END_DATE}.parquet"
RATE_LIMIT = 0.35                                           # seconds between requests
USER_AGENT = "short-sale-scraper/0.1 (+github.com/anusheel-sharma"

# ----------- 2. DOWNLOAD ONE DAY AND SAVE ----------------------------------------------
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

def save_to_parquet(
        df: pd.DataFrame,
        out_path: str | Path,
        compression: str = "snappy",
        overwrite: bool = True
) -> None:

    """
    Save *df* to a Parquet file.

    Parameters
    ----------
    df : pandas.DataFrame
        The table you want to persist.
    out_path : str or Path
        Where to write the file, e.g. 'data/commodities.parquet'.
    compression : {'snappy','zstd','brotli','gzip',None}
        Codec passed to pandas.to_parquet (default = snappy, best mix of speed/size).
    overwrite : bool
        If False and the file exists, raise FileExistsError.

    Notes
    -----
    • Requires 'pyarrow' ≥ 12.0 (or 'fastparquet') in your environment.
    • Creates parent folders if they don’t exist.
    • Writes with *index=False* so the filesystem is your only index.
    """

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"{out_path} already exists (set overwrite=True to replace)")

    df.to_parquet(out_path, engine="pyarrow", compression=compression, index=False)
    print(f"✅  Saved {len(df):,} rows  ➜  {out_path}")


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
save_to_parquet(master, OUTPUT)
print(f"\nSaved {len(master):,} rows ➜ {OUTPUT}")
