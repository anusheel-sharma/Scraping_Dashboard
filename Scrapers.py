import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from typing import Dict, List, Optional

class YahooFinanceAnalysisScraper:
    def __init__(self):
        # Set up headers to mimic a real browser request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape_ticker_analysis(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """
        Scrapes analysis data for a given stock ticker.
        
        Args:
            ticker (str): Stock ticker symbol (e.g., 'PG', 'AAPL')
        
        Returns:
            Dict[str,pd.DataFrame]: Dictionary containing scraped tables
        """
        url = f"https://uk.finance.yahoo.com/quote/{ticker}/analysis/"

        try:
            # Make the request
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Dictionary to store all scraped tables
            tables_data = {}

            sections = [
                'earningsEstimate', 
                'earningsHistory',
                'revenueEstimate',
                'epsRevisions',
                'epsTrend',
                'growthEstimate'
            ]
                
            # Find tables for different sections
            for section_name in sections:
                    section = soup.find('section', {'data-testid': section_name})
                    if section:
                        table = self._extract_table_from_section(section)
                        if table is not None:
                            tables_data[section_name] = table
            
            return tables_data
            

        except requests.RequestException as e:
            print(f"Error fetching data for {ticker}: {e}")
            return {}
        except Exception as e:
            print(f"Error processing data for {ticker}: {e}")
            return {}

    def _extract_table_from_section(self, section) -> Optional[pd.DataFrame]:
        """
        Extract table data from a section element
        Args:
            section: BeautifulSoup element containing the section
        
        Returns:
            pd.DataFrame or None: Extracted table data
        
        """

        try:
            # Find table within the section
            table = section.find('table')
            if not table:
                 return None
            
            # Extract headers
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all('th')]

            # Extract data rows
            rows_data = []
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('trow')
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        rows_data.append(row_data)

            if headers and rows_data:
                df = pd.DataFrame(rows_data, columns=headers)
                return df

        except Exception as e:
             print(f"Error extracting table: {e}")
             return None

    def scrape_multiple_tickers(self, tickers: List[str], delay: float = 1.0) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Scrapes analysis data for multiple tickers.

        Args:
            tickers (List[str]): List of ticker symboles
            delay (float): Delay between requests in seconds
        
        Returns:
            Dict[str,Dict[str,pd.DataFrame]]: Nested dictionary with ticker -> table_name -> DataFrame
        """

        all_data = {}
        for ticker in tickers:
            print(f"Scraping data for {ticker}...")
            ticker_data = self.scrape_ticker_analysis(ticker)
            if ticker_data:
                all_data[ticker] = ticker_data
                print(f"Successfully scraped {len(ticker_data)} tables for {ticker}")
            else:
                print(f"No data found for {ticker}")

            # Add delay to be respectful to the server
            time.sleep(delay)
        
        return all_data
