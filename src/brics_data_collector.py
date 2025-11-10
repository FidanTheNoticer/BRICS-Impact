"""
Data collection module for BRICS economic indicators.
"""

import pandas as pd
import requests
from datetime import datetime
import logging
from typing import Dict, List, Optional
import time

class BRICSEconomicData:
    """
    Collects economic data for BRICS countries from World Bank API.
    """
    
    def __init__(self):
        self.brics_countries = {
            'BR': 'Brazil',
            'RU': 'Russia', 
            'IN': 'India',
            'CN': 'China',
            'ZA': 'South Africa'
        }
        
        # World Bank indicators for economic data
        self.indicators = {
            'gdp': 'NY.GDP.MKTP.CD',        # GDP (current US$)
            'gdp_growth': 'NY.GDP.MKTP.KD.ZG',  # GDP growth (annual %)
            'inflation': 'FP.CPI.TOTL.ZG',      # Inflation, consumer prices (annual %)
            'trade': 'NE.TRD.GNFS.ZS',          # Trade (% of GDP)
            'exports': 'NE.EXP.GNFS.CD',        # Exports of goods and services (current US$)
            'reserves': 'FI.RES.TOTL.CD',       # Total reserves (includes gold, current US$)
        }
        
        self.base_url = "http://api.worldbank.org/v2"
        self.logger = logging.getLogger(__name__)
    
    def fetch_worldbank_indicator(self, indicator: str, country: str, country_name: str) -> pd.DataFrame:
        """
        Fetch data from World Bank API for a specific indicator and country.
        """
        try:
            url = f"{self.base_url}/country/{country}/indicator/{indicator}?format=json&per_page=100"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                if len(data) > 1 and data[1]:
                    records = []
                    for entry in data[1]:
                        if entry['value'] is not None:
                            records.append({
                                'year': int(entry['date']),
                                'value': entry['value'],
                                'country': country_name,
                                'country_code': country,
                                'indicator': indicator
                            })
                    
                    if records:
                        return pd.DataFrame(records)
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"✗ Error fetching {indicator} for {country_name}: {str(e)}")
            return pd.DataFrame()
    
    def fetch_all_countries_indicator(self, indicator: str, indicator_name: str) -> pd.DataFrame:
        """
        Fetch indicator data for all BRICS countries.
        """
        print(f"Fetching {indicator_name} from World Bank...")
        
        all_data = []
        for country_code, country_name in self.brics_countries.items():
            data = self.fetch_worldbank_indicator(indicator, country_code, country_name)
            if not data.empty:
                all_data.append(data)
                print(f"✓ {country_name} - {len(data)} years of data")
            else:
                print(f"✗ {country_name} - No data available")
            
            # Small delay to be respectful to the API
            time.sleep(0.5)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def collect_all_economic_data(self) -> Dict[str, pd.DataFrame]:
        """
        Collect all economic indicators for BRICS countries.
        """
        print("Starting comprehensive BRICS economic data collection...")
        
        economic_data = {}
        
        for indicator_name, indicator_code in self.indicators.items():
            print(f"\n--- Collecting {indicator_name.upper()} ---")
            data = self.fetch_all_countries_indicator(indicator_code, indicator_name)
            
            if not data.empty:
                economic_data[indicator_name] = data
                print(f"✅ {indicator_name} collected successfully")
            else:
                print(f"❌ Failed to collect {indicator_name}")
        
        return economic_data
    
    def save_economic_data(self, economic_data: Dict[str, pd.DataFrame]):
        """
        Save all economic data to CSV files.
        """
        print("\n--- Saving Economic Data ---")
        
        for indicator_name, data in economic_data.items():
            if not data.empty:
                filename = f"data/raw/brics_{indicator_name}.csv"
                data.to_csv(filename, index=False)
                print(f"✓ {indicator_name} saved to {filename}")
    
    def create_summary_report(self, economic_data: Dict[str, pd.DataFrame]):
        """
        Create a summary report of the collected data.
        """
        print("\n--- Economic Data Summary ---")
        
        for indicator_name, data in economic_data.items():
            if not data.empty:
                latest_year = data['year'].max()
                countries_with_data = data['country'].nunique()
                total_records = len(data)
                print(f"{indicator_name.upper()}: {countries_with_data}/5 countries, {total_records} records, up to {latest_year}")

def main():
    """Test the BRICS economic data collector"""
    collector = BRICSEconomicData()
    
    # Collect all economic data
    economic_data = collector.collect_all_economic_data()
    
    # Save data
    collector.save_economic_data(economic_data)
    
    # Create summary
    collector.create_summary_report(economic_data)
    
    if economic_data:
        print("\n🎉 BRICS economic data collection completed!")
        total_indicators = len([d for d in economic_data.values() if not d.empty])
        print(f"📊 Collected {total_indicators} economic indicators")
    else:
        print("\n❌ No data was collected. Please check your internet connection.")

if __name__ == "__main__":
    main()