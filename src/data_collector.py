"""
Data collection module for BRICS and commodity market data.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
import logging
from typing import Dict, List, Optional

class BRICSAnalyzer:
    """
    Main class for BRICS currency impact analysis.
    Handles data collection and basic processing.
    """
    
    def __init__(self):
        self.brics_countries = {
            'BR': 'Brazil', 
            'RU': 'Russia', 
            'IN': 'India', 
            'CN': 'China', 
            'ZA': 'South Africa'
        }
        self.logger = logging.getLogger(__name__)
    
    def fetch_commodity_prices(self) -> pd.DataFrame:
        """
        Fetch basic commodity prices from Yahoo Finance.
        
        Returns:
            DataFrame with commodity prices
        """
        print("Fetching commodity prices from Yahoo Finance...")
        
        # Symbols for commodities in Yahoo Finance
        commodity_symbols = {
            'CL=F': 'Crude_Oil',      # Crude Oil Futures
            'GC=F': 'Gold',           # Gold Futures
            'HG=F': 'Copper',         # Copper Futures
            'ZC=F': 'Corn'            # Corn Futures
        }
        
        try:
            end_date = datetime.now()
            start_date = datetime(2000, 1, 1)  # Data depuis l'an 2000
            
            all_data = []
            for symbol, name in commodity_symbols.items():
                print(f"Downloading {name}...")
                data = yf.download(symbol, start=start_date, end=end_date, progress=False)
                if not data.empty:
                    data = data[['Close']].rename(columns={'Close': name})
                    all_data.append(data)
            
            if all_data:
                # Combine all commodities into one DataFrame
                commodity_data = pd.concat(all_data, axis=1)
                print("✓ Commodity prices fetched successfully")
                return commodity_data.dropna()
            else:
                print("✗ No data received")
                return pd.DataFrame()
            
        except Exception as e:
            print(f"✗ Error fetching commodity prices: {e}")
            return pd.DataFrame()
    
    def test_connection(self):
        """Test if we can fetch some data"""
        print("Testing connection to financial data sources...")
        data = self.fetch_commodity_prices()
        
        if not data.empty:
            print(f"✅ Success! Collected {len(data)} days of data")
            print(f"Commodities: {list(data.columns)}")
            print(f"Date range: {data.index[0]} to {data.index[-1]}")
            print("\nFirst few rows:")
            print(data.head())
            
            # Save to CSV for verification
            data.to_csv('data/raw/commodity_prices.csv')
            print("✓ Data saved to 'data/raw/commodity_prices.csv'")
        else:
            print("❌ Failed to collect data")

def main():
    """Test the data collector"""
    analyzer = BRICSAnalyzer()
    analyzer.test_connection()

if __name__ == "__main__":
    main()