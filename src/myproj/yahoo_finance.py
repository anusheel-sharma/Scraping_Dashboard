
"""
Yahoo Finance API Equity and Commodity Price Download
=====================================================

Downloads prices for S&P 500 equities from 01-01-2020 to today
Downloads commodity prices (XXXX) from 01-01-2020 to today
Returns one DataFrame with all data combined and exports to csv
"""


import pandas as pd
import yfinance as yf
from typing import List
import time
import datetime as dt
from pathlib import Path

# =============================== 1. CONFIG =====================================
START_DATE = dt.date(2019, 1, 1)
END_DATE = dt.date(2025, 7, 7)
COMMOD_TICKERS = [
    "GLD", "SLV", "CPER",               # Gold, Silver, Copper,
    "UNG", "UGA", "BNO",                # energy feed-stocks - Natural gas, Gasoline, US Brent Oil
    "ALUM", "PPLT", "PALL",             # metals - Aluminium, Platinum, Palladium
    "CORN", "WEAT", "SOYB", "CANE",     # grains & sugar - Corn, Wheat, Soybeans, Sugar
    "COTN.L", "COFF.L"                  # cotton, coffee
]

# =============================== 2. CORE FUNCTIONS =====================================

def get_sp500_list() -> List[str]:
    """
    Using a table from Wikipedia, download a list of S&P500 company tickers

    Returns:
        List[str]: A list of tickers for constituents of S&P 500 

    """

    # Gather list of S&P 500 tickers from wikipedia
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)

    # Extract ticker symbols from wiki table and convert to list
    sp500_table = tables[0]
    sp500_symbols = sp500_table['Symbol']
    tickers = sp500_symbols.tolist()

    return tickers

def download_ticker_data(tickers: List[str]) -> pd.DataFrame:
    """
    Create a consolidated DataFrame of daily price data for each ticker in list.
    Data for each ticker is stacked on top of another

    Args:
        tickers: List of tickers to download data for
    
    Returns:
        pd.DataFrame: Consolidated DataFrame of daily price data for each ticker in list.
    """

    frames = []
    if len(tickers) > 30:
        chunksize = 30
    else:
        chunksize = len(tickers)
    
    for i in range(0, len(tickers), chunksize):
        part = tickers[i:i+chunksize]
        print(f"fetching {len(part)} tickers…")

        # Set auto_adjust=False to retrieve Adjusted Closing Prices
        df = yf.download(tickers=part, start=START_DATE, end=END_DATE, auto_adjust=False, threads=True)
        frames.append(df)
        time.sleep(0.4)

    wide = pd.concat(frames, axis=1)

    # Stack takes level 1 column index (Ticker) and unpivots to create a new column for ticker
    tidy = wide.stack(level=1, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index()

    return tidy

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


# =============================== 3. MAIN FUNCTION =====================================

# Download equity and commodities data
sp_tickers = get_sp500_list()
equities = download_ticker_data(sp_tickers)
commodities = download_ticker_data(COMMOD_TICKERS)

# Combine dfs and export to parquet
all_prices = pd.concat([equities, commodities], axis=0)
save_to_parquet(all_prices, 'data/raw/equity_commodity_data.parquet')