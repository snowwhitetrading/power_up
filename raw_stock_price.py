"""
Script to merge hydro, gas, and coal stock lists and create a CSV file
with timestamp, close, and symbol columns using SSI API
"""

import pandas as pd
from datetime import datetime, timedelta
import os
from ssi_api import get_stock_data_batch, fetch_historical_price

def create_merged_stocks_csv():
    """
    Merge hydro, gas, and coal stock lists and create CSV with timestamp, close, symbol
    """
    
    # Define stock lists
    hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP','TBC']
    gas_stocks = ['POW', 'NT2']
    coal_stocks = ['QTP', 'PPC', 'HND']
    
    # Create mapping of stocks to their types
    stock_type_mapping = {}
    for stock in hydro_stocks:
        stock_type_mapping[stock] = 'hydro'
    for stock in gas_stocks:
        stock_type_mapping[stock] = 'gas'
    for stock in coal_stocks:
        stock_type_mapping[stock] = 'coal'
    
    # Merge all stocks into one list
    all_stocks = hydro_stocks + gas_stocks + coal_stocks
    
    print(f"Total stocks to process: {len(all_stocks)}")
    print(f"Stocks: {all_stocks}")
    
    # Set date range (last 2 years for comprehensive data)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365*15)).strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date} to {end_date}")
    
    # List to store all data
    all_data = []
    
    # Progress tracking
    total_stocks = len(all_stocks)
    successful = 0
    failed = 0
    
    for i, symbol in enumerate(all_stocks, 1):
        print(f"Processing {symbol} ({i}/{total_stocks})...")
        
        try:
            # Fetch historical data for the stock
            df = fetch_historical_price(symbol, start_date, end_date)
            
            if df is not None and not df.empty and 'close' in df.columns:
                # Add symbol column
                df['symbol'] = symbol
                
                # Add type column based on stock category
                df['type'] = stock_type_mapping[symbol]
                
                # Rename time column to timestamp for clarity
                if 'time' in df.columns:
                    df = df.rename(columns={'time': 'timestamp'})
                
                # Select only required columns: timestamp, close, symbol, type
                df_selected = df[['timestamp', 'close', 'symbol', 'type']].copy()
                
                # Convert timestamp to proper datetime format
                df_selected['timestamp'] = pd.to_datetime(df_selected['timestamp'])
                
                # Add to main data list
                all_data.append(df_selected)
                successful += 1
                print(f"✓ Successfully fetched {len(df_selected)} records for {symbol}")
                
            else:
                print(f"✗ No data available for {symbol}")
                failed += 1
                
        except Exception as e:
            print(f"✗ Error fetching data for {symbol}: {e}")
            failed += 1
    
    # Combine all data
    if all_data:
        print(f"\nCombining data from {successful} successful fetches...")
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Sort by timestamp and symbol
        combined_df = combined_df.sort_values(['timestamp', 'symbol']).reset_index(drop=True)
        
        # Save to CSV
        output_file = 'data/raw_stock_price.csv'
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        
        combined_df.to_csv(output_path, index=False)
        
        print(f"\n✓ Successfully created CSV file: {output_file}")
        print(f"Total records: {len(combined_df)}")
        print(f"Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
        print(f"Stocks included: {sorted(combined_df['symbol'].unique())}")
        
        # Display sample data
        print(f"\nSample data (first 10 rows):")
        print(combined_df.head(10).to_string(index=False))
        
        # Show summary statistics
        print(f"\nSummary by stock:")
        summary = combined_df.groupby('symbol').agg({
            'close': ['count', 'min', 'max', 'mean'],
            'timestamp': ['min', 'max']
        }).round(2)
        print(summary)
        
        # Show summary by type
        print(f"\nSummary by type:")
        type_summary = combined_df.groupby('type').agg({
            'symbol': 'nunique',
            'close': 'count'
        })
        type_summary.columns = ['unique_stocks', 'total_records']
        print(type_summary)
        
        return output_path
        
    else:
        print("✗ No data was successfully fetched for any stocks")
        return None

if __name__ == "__main__":
    output_file = create_merged_stocks_csv()
    if output_file:
        print(f"\n🎉 Process completed! CSV file saved at: {output_file}")
    else:
        print("\n❌ Process failed - no data was fetched")