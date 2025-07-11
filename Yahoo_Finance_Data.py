
"""
Yahoo Finance API Equity and Commodity Price Download
=====================================================

Downloads prices for S&P 500 equities from 01-01-2020 to today
Downloads commodity prices (XXXX) from 01-01-2020 to today
Returns one DataFrame with 
"""

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

import pandas as pd
import yfinance as yf
from typing import List
import time
import datetime as dt

# =============================== 1. CONFIG =====================================
START = dt.date(2024, 1, 1)
END = dt.date(2025, 7, 7)

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
    chunksize = 30
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


# =============================== 3. MAIN LOOP =====================================

