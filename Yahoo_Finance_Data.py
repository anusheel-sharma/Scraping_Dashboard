
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

# =============================== 1. CONFIG =====================================
START_DATE = dt.date(2024, 1, 1)
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


# =============================== 3. MAIN FUNCTION =====================================

# Download equity and commodities data
sp_tickers = get_sp500_list()
equities = download_ticker_data(sp_tickers)
commodities = download_ticker_data(COMMOD_TICKERS)

all_prices = pd.concat([equities, commodities], axis=0)