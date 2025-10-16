"""
Gas Strategy Module - POW vs NT2 Quarterly Portfolio Strategy
Based on quarterly YoY growth of contracted volumes from 1Q2019 to 3Q2025
Note: Ca Mau contracted volume is 0 from 2019-2021, but is included in POW total contracted volume calculation

Methodology:
- Diversified portfolio (20% threshold strategy):
  + If POW growth - NT2 growth > 20%, invest 100% in POW in the next quarter
  + If NT2 growth - POW growth > 20%, invest 100% in NT2 in the next quarter  
  + Otherwise, use equal weight (50/50)
  
- Concentrated portfolio (no threshold):
  + Invest 100% in the stock with higher contracted volume YoY growth from previous quarter
  + Uses forward-looking approach: previous quarter's growth determines current quarter's allocation
"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Optional, Tuple, Dict, Any
import os

# Try to import ssi_api, make it optional
try:
    import ssi_api
    SSI_API_AVAILABLE = True
except ImportError:
    SSI_API_AVAILABLE = False
    print("Warning: ssi_api module not available. Gas strategy will use mock data.")

def load_vni_data():
    """Load VNI data from CSV file and convert to quarterly returns"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vni_file = os.path.join(script_dir, 'data', 'vn_index_monthly.csv')
        
        vni_df = pd.read_csv(vni_file)
        
        # The VNI file structure: first column is period, second is VNINDEX value
        if len(vni_df.columns) >= 2:
            vni_df.columns = ['period', 'close'] + list(vni_df.columns[2:])
        else:
            st.error("VNI file does not have expected structure")
            return []
        
        # Clean and filter data - exclude header rows
        vni_df = vni_df.dropna(subset=['period', 'close'])
        
        # Remove header rows like 'date', 'period', etc.
        vni_df = vni_df[~vni_df['period'].astype(str).str.lower().isin(['date', 'period', 'time'])]
        
        # Convert close to numeric, handle commas
        vni_df['close'] = vni_df['close'].astype(str).str.replace(',', '')
        vni_df['close'] = pd.to_numeric(vni_df['close'], errors='coerce')
        vni_df = vni_df.dropna(subset=['close'])
        
        # Ensure period format is consistent and convert from "1Q2019" to "2019Q1" format
        vni_df['period'] = vni_df['period'].astype(str).str.strip()
        
        # Convert period format from "1Q2019" to "2019Q1" if needed
        def convert_period_format(period_str):
            try:
                period_str = str(period_str).strip()
                if 'Q' in period_str and len(period_str) > 3:
                    parts = period_str.split('Q')
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        quarter = parts[0]
                        year = parts[1]
                        return f"{year}Q{quarter}"
                return period_str
            except:
                return period_str
        
        vni_df['period'] = vni_df['period'].apply(convert_period_format)
        
        # Filter to only valid quarterly periods
        vni_df = vni_df[vni_df['period'].str.contains(r'\d{4}Q\d', na=False)]
        
        # Sort by period
        vni_df = vni_df.sort_values('period')
        
        # Calculate quarterly returns
        vni_df['quarterly_return'] = vni_df['close'].pct_change() * 100
        
        # Filter to target period (2019Q1 to 2025Q3)
        vni_filtered = vni_df[vni_df['period'].between('2019Q1', '2025Q3')]
        
        # Convert to list of dictionaries
        return vni_filtered[['period', 'quarterly_return']].dropna().to_dict('records')
        
    except Exception as e:
        st.error(f"Error loading VNI data: {e}")
        return []

def load_pvpower_data():
    """Load PVPower monthly data from CSV file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, 'data',  'volume_pow_monthly.csv')
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading PVPower data: {str(e)}")
        return None

def process_quarterly_data(df):
    """Convert monthly data to quarterly and calculate YoY growth for POW and NT2 contracted volumes"""
    try:
        if df is None or df.empty:
            return None
        
        # Ensure we have a Date column
        if 'Date' not in df.columns:
            # Try to create date from Year, Month columns if they exist
            if 'Year' in df.columns and 'Month' in df.columns:
                df['Date'] = pd.to_datetime([f"{int(y)}-{int(m):02d}-01" for y, m in zip(df['Year'], df['Month'])])
            # Try other common date column patterns
            elif any(col.lower() in ['date', 'datetime', 'time'] for col in df.columns):
                date_col = next(col for col in df.columns if col.lower() in ['date', 'datetime', 'time'])
                df['Date'] = pd.to_datetime(df[date_col])
            # Try to infer from index if it's a datetime index
            elif hasattr(df.index, 'year'):
                df['Date'] = df.index
            else:
                st.error("No Date, Year/Month columns found in the data")
                st.write("Available columns:")
                st.write(df.columns.tolist())
                return None
        
        # Convert Date to datetime if it's not already
        df['Date'] = pd.to_datetime(df['Date'])
        df['Year'] = df['Date'].dt.year
        df['Quarter'] = df['Date'].dt.quarter
        
        # Filter data from 1Q2019 to 3Q2025 (include 2019-2021 data where Ca Mau has 0 contracted volume)
        # Note: Ca Mau contracted volume is 0 from 2019-2021, but we still include it in POW total
        start_date = pd.to_datetime('2019-01-01')
        end_date = pd.to_datetime('2025-09-30')
        df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        
        # Find POW Contracted and NT2 contracted volume columns
        # Look for direct POW Contracted column first
        pow_contracted_cols = [col for col in df.columns if 'POW' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
        nt2_cols = [col for col in df.columns if 'NT2' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
        
        # If NT2 columns not found, look for alternatives
        if not nt2_cols:
            nt2_cols = [col for col in df.columns if 'NHON TRACH 2' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
            
        if not pow_contracted_cols or not nt2_cols:
            st.error("Could not find POW Contracted and NT2 contracted volume columns")
            st.write("Available columns:")
            st.write(df.columns.tolist())
            return None
            
        # Use the POW Contracted column directly (not summing individual plants)
        pow_col = pow_contracted_cols[0]
        
        # Show sample POW contracted data
        sample_pow = df[pow_col].head(10)
        
        nt2_col = nt2_cols[0]
        
        # Show sample NT2 data for comparison
        sample_nt2 = df[nt2_col].dropna().head(5)
        
        # Convert to numeric
        df[pow_col] = pd.to_numeric(df[pow_col], errors='coerce')
        df[nt2_col] = pd.to_numeric(df[nt2_col], errors='coerce')
        
        # Group by quarter and sum the volumes
        quarterly_df = df.groupby(['Year', 'Quarter']).agg({
            pow_col: 'sum',
            nt2_col: 'sum'
        }).reset_index()
        
        # Create quarter label safely
        quarterly_df['Quarter_Label'] = quarterly_df.apply(lambda row: f"{int(row['Year'])}Q{int(row['Quarter'])}", axis=1)
        quarterly_df['Date'] = pd.to_datetime([f"{int(y)}-{int(q)*3:02d}-01" for y, q in zip(quarterly_df['Year'], quarterly_df['Quarter'])])
        
        # Calculate YoY growth (4 quarters back)
        quarterly_df = quarterly_df.sort_values(['Year', 'Quarter']).reset_index(drop=True)
        quarterly_df['POW_YoY_Growth'] = quarterly_df[pow_col].pct_change(periods=4) * 100
        quarterly_df['NT2_YoY_Growth'] = quarterly_df[nt2_col].pct_change(periods=4) * 100
        
        # Rename columns for clarity
        quarterly_df = quarterly_df.rename(columns={
            pow_col: 'POW_Contracted',
            nt2_col: 'NT2_Contracted'
        })
        
        return quarterly_df
        
    except Exception as e:
        st.error(f"Error processing quarterly data: {str(e)}")
        return None

def construct_portfolio_strategy(quarterly_df):
    """
    Construct quarterly portfolio based on YoY growth comparison
    Strategy starts from 2Q2019 based on 1Q2019 YoY growth comparison
    Data includes 2019-2021 period where Ca Mau contracted volume is 0, but is still included in POW total
    
    New Methodology:
    - If POW growth - NT2 growth > 20%, invest 100% in POW in the next quarter
    - If NT2 growth - POW growth > 20%, invest 100% in NT2 in the next quarter  
    - Otherwise, use equal weight (50/50)
    """
    try:
        if quarterly_df is None or quarterly_df.empty:
            return None
            
        strategy_df = quarterly_df.copy()
        
        # Strategy starts from 2Q2019 (based on 1Q2019 YoY growth)
        start_strategy_date = pd.to_datetime('2019-04-01')  # 2Q2019
        
        # Portfolio allocation logic: invest 100% in the stock with higher YoY growth difference > 20%
        # Investment decision is made for next quarter based on current quarter comparison
        strategy_df['Portfolio_Decision'] = 'Hold'  # Default
        strategy_df['POW_Weight'] = 0.5  # Default equal weight
        strategy_df['NT2_Weight'] = 0.5  # Default equal weight
        
        for i in range(len(strategy_df)-1):  # Exclude last row as we can't invest for next quarter
            current_row = strategy_df.iloc[i]
            next_row_date = strategy_df.iloc[i+1]['Date']
            
            # Only apply strategy starting from 2Q2019
            if next_row_date < start_strategy_date:
                # Before 2Q2019, use equal weights (50/50)
                strategy_df.loc[i+1, 'Portfolio_Decision'] = 'Equal'
                strategy_df.loc[i+1, 'POW_Weight'] = 0.5
                strategy_df.loc[i+1, 'NT2_Weight'] = 0.5
                continue
            
            # Compare YoY growth rates (only if both are not NaN)
            if not pd.isna(current_row['POW_YoY_Growth']) and not pd.isna(current_row['NT2_YoY_Growth']):
                pow_growth = current_row['POW_YoY_Growth']
                nt2_growth = current_row['NT2_YoY_Growth']
                
                # Calculate growth difference
                pow_advantage = pow_growth - nt2_growth
                nt2_advantage = nt2_growth - pow_growth
                
                # Apply new methodology: 20% threshold for 100% allocation
                if pow_advantage > 20:
                    # POW growth advantage > 20%, invest 100% in POW
                    strategy_df.loc[i+1, 'Portfolio_Decision'] = 'POW'
                    strategy_df.loc[i+1, 'POW_Weight'] = 1.0
                    strategy_df.loc[i+1, 'NT2_Weight'] = 0.0
                elif nt2_advantage > 20:
                    # NT2 growth advantage > 20%, invest 100% in NT2
                    strategy_df.loc[i+1, 'Portfolio_Decision'] = 'NT2'
                    strategy_df.loc[i+1, 'POW_Weight'] = 0.0
                    strategy_df.loc[i+1, 'NT2_Weight'] = 1.0
                else:
                    # Growth difference <= 20%, use equal weight
                    strategy_df.loc[i+1, 'Portfolio_Decision'] = 'Equal'
                    strategy_df.loc[i+1, 'POW_Weight'] = 0.5
                    strategy_df.loc[i+1, 'NT2_Weight'] = 0.5
            else:
                # If YoY data not available (likely for POW due to missing historical data)
                # Use equal weight or last known strategy
                strategy_df.loc[i+1, 'Portfolio_Decision'] = 'Equal'
                strategy_df.loc[i+1, 'POW_Weight'] = 0.5
                strategy_df.loc[i+1, 'NT2_Weight'] = 0.5
        
        # Add next quarter decision based on last quarter's data (for forward-looking portfolio)
        # Example: Use 2025Q2 growth to determine 2025Q3 portfolio allocation
        if len(strategy_df) > 0:
            last_row = strategy_df.iloc[-1]
            last_date = last_row['Date']
            last_quarter_label = last_row['Quarter_Label']
            
            # Calculate next quarter
            next_quarter_num = last_row['Quarter'] + 1
            next_year = last_row['Year']
            if next_quarter_num > 4:
                next_quarter_num = 1
                next_year += 1
            
            next_quarter_label = f"{next_year}Q{next_quarter_num}"
            next_date = pd.to_datetime(f"{next_year}-{next_quarter_num*3:02d}-01")  # First day of next quarter
            
            # Make decision for next quarter based on current (last) quarter's growth
            pow_weight = 0.5
            nt2_weight = 0.5
            decision = 'Equal'
            
            if not pd.isna(last_row['POW_YoY_Growth']) and not pd.isna(last_row['NT2_YoY_Growth']):
                pow_growth = last_row['POW_YoY_Growth']
                nt2_growth = last_row['NT2_YoY_Growth']
                pow_advantage = pow_growth - nt2_growth
                nt2_advantage = nt2_growth - pow_growth
                
                if pow_advantage > 20:
                    pow_weight = 1.0
                    nt2_weight = 0.0
                    decision = 'POW'
                elif nt2_advantage > 20:
                    pow_weight = 0.0
                    nt2_weight = 1.0
                    decision = 'NT2'
            
            # Create new row for next quarter
            new_row = pd.DataFrame([{
                'Date': next_date,
                'Year': next_year,
                'Quarter': next_quarter_num,
                'Quarter_Label': next_quarter_label,
                'POW_Contracted': np.nan,  # No volume data for future quarter
                'NT2_Contracted': np.nan,
                'POW_YoY_Growth': np.nan,
                'NT2_YoY_Growth': np.nan,
                'Portfolio_Decision': decision,
                'POW_Weight': pow_weight,
                'NT2_Weight': nt2_weight
            }])
            
            strategy_df = pd.concat([strategy_df, new_row], ignore_index=True)
        
        return strategy_df
        
    except Exception as e:
        st.error(f"Error constructing portfolio strategy: {str(e)}")
        return None

def get_stock_returns_ssi(tickers=['POW', 'NT2'], start_year=2019, end_year=2025):
    """
    Get stock returns from raw_stock_price.csv file
    Start from 2019 to align with gas strategy timing
    """
    try:
        stock_data = {}
        
        # Load data from CSV file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data', 'raw_stock_price.csv')
        
        if not os.path.exists(csv_file):
            st.error(f"❌ CSV file not found: {csv_file}")
            return {}
        
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter for gas stocks only (since this is gas strategy)
        gas_df = df[df['type'] == 'gas'].copy()
        
        # Filter for requested symbols and date range
        available_symbols = gas_df['symbol'].unique()
        requested_symbols = [symbol for symbol in tickers if symbol in available_symbols]
        
        if not requested_symbols:
            st.warning(f"❌ No gas stocks found in CSV for symbols: {tickers}")
            return {}
        
        # Filter by date range
        start_date = pd.to_datetime(f"{start_year}-01-01")
        end_date = pd.to_datetime(f"{end_year}-12-31")
        gas_df = gas_df[(gas_df['timestamp'] >= start_date) & (gas_df['timestamp'] <= end_date)]
        
        # Process each stock
        for ticker in requested_symbols:
            try:
                ticker_data = gas_df[gas_df['symbol'] == ticker].copy()
                
                if not ticker_data.empty:
                    # Sort by date and set as index for resampling
                    ticker_data = ticker_data.sort_values('timestamp')
                    ticker_data = ticker_data.set_index('timestamp')
                    
                    # Resample to quarterly using last price of quarter (quarter-end)
                    quarterly_data = ticker_data['close'].resample('Q').last().to_frame()
                    quarterly_data.columns = [f'{ticker}_Price']
                    quarterly_data = quarterly_data.reset_index()
                    
                    # Create Quarter_Label to match strategy data format
                    quarterly_data['Year'] = quarterly_data['timestamp'].dt.year
                    quarterly_data['Quarter'] = quarterly_data['timestamp'].dt.quarter
                    quarterly_data['Quarter_Label'] = quarterly_data['Year'].astype(str) + 'Q' + quarterly_data['Quarter'].astype(str)
                    
                    # Calculate quarterly returns
                    quarterly_data[f'{ticker}_Return'] = quarterly_data[f'{ticker}_Price'].pct_change() * 100
                    
                    # Create final format expected by portfolio calculation
                    stock_returns = quarterly_data[['Quarter_Label', f'{ticker}_Return']].copy()
                    stock_returns.columns = ['Quarter', 'Return']
                    
                    # Remove NaN values (first row will have NaN return)
                    stock_returns = stock_returns.dropna()
                    
                    stock_data[ticker] = stock_returns
                    
            except Exception as e:
                st.warning(f"Could not process data for {ticker}: {e}")
                continue
        
        if not stock_data:
            st.error("❌ Failed to load any stock data from CSV")
        else:
            st.success(f"✅ Loaded data for {len(stock_data)} gas stocks from CSV")
            
        return stock_data
        
    except Exception as e:
        st.error(f"Error getting stock returns: {str(e)}")
        return {}

def calculate_portfolio_returns(strategy_df, stock_data):
    """Calculate portfolio returns vs equally weighted and VNI returns"""
    try:
        if strategy_df is None or not stock_data:
            return None
            
        # Merge strategy data with stock returns and volume growth data
        returns_df = strategy_df[['Quarter_Label', 'POW_Weight', 'NT2_Weight', 'Portfolio_Decision', 
                                   'POW_YoY_Growth', 'NT2_YoY_Growth']].copy()
        
        # Add stock returns
        if 'POW' in stock_data and 'NT2' in stock_data:
            pow_returns = stock_data['POW'].set_index('Quarter')['Return'] if 'Quarter' in stock_data['POW'].columns else pd.Series()
            nt2_returns = stock_data['NT2'].set_index('Quarter')['Return'] if 'Quarter' in stock_data['NT2'].columns else pd.Series()
            
            returns_df['POW_Return'] = returns_df['Quarter_Label'].map(pow_returns)
            returns_df['NT2_Return'] = returns_df['Quarter_Label'].map(nt2_returns)
            
            # Fill NaN with 0
            returns_df['POW_Return'] = returns_df['POW_Return'].fillna(0)
            returns_df['NT2_Return'] = returns_df['NT2_Return'].fillna(0)
            
            # Calculate strategy portfolio return (existing strategy with 20% threshold)
            returns_df['Strategy_Return'] = (returns_df['POW_Weight'] * returns_df['POW_Return'] + 
                                           returns_df['NT2_Weight'] * returns_df['NT2_Return'])
            
            # Calculate equally weighted return
            returns_df['Equal_Weight_Return'] = (0.5 * returns_df['POW_Return'] + 
                                               0.5 * returns_df['NT2_Return'])
            
            # NEW: Calculate "Concentrated" portfolio - 100% in stock with higher contracted volume YoY growth
            # This uses PREVIOUS quarter's volume growth to decide CURRENT quarter's allocation (forward-looking)
            returns_df['Concentrated_Return'] = 0.0
            returns_df['Concentrated_POW_Weight'] = 0.5  # Default
            returns_df['Concentrated_NT2_Weight'] = 0.5  # Default
            
            for i in range(len(returns_df)):
                # For first quarter, use equal weight
                if i == 0:
                    returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_Return')] = (
                        0.5 * returns_df.iloc[i]['POW_Return'] + 
                        0.5 * returns_df.iloc[i]['NT2_Return']
                    )
                    returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_POW_Weight')] = 0.5
                    returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_NT2_Weight')] = 0.5
                else:
                    # Use previous quarter's volume growth to decide current quarter allocation
                    prev_pow_growth = returns_df.iloc[i-1]['POW_YoY_Growth']
                    prev_nt2_growth = returns_df.iloc[i-1]['NT2_YoY_Growth']
                    
                    # Select stock with higher volume growth from previous quarter
                    if pd.notna(prev_pow_growth) and pd.notna(prev_nt2_growth):
                        if prev_pow_growth >= prev_nt2_growth:
                            # Invest 100% in POW based on previous quarter's higher growth
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_Return')] = returns_df.iloc[i]['POW_Return']
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_POW_Weight')] = 1.0
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_NT2_Weight')] = 0.0
                        else:
                            # Invest 100% in NT2 based on previous quarter's higher growth
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_Return')] = returns_df.iloc[i]['NT2_Return']
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_POW_Weight')] = 0.0
                            returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_NT2_Weight')] = 1.0
                    else:
                        # If volume growth data not available, use equal weight
                        returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_Return')] = (
                            0.5 * returns_df.iloc[i]['POW_Return'] + 
                            0.5 * returns_df.iloc[i]['NT2_Return']
                        )
                        returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_POW_Weight')] = 0.5
                        returns_df.iloc[i, returns_df.columns.get_loc('Concentrated_NT2_Weight')] = 0.5
            
            # Load real VNI return data
            vni_data = load_vni_data()
            
            # Create VNI return mapping
            vni_return_map = {}
            for vni_record in vni_data:
                vni_return_map[vni_record['period']] = vni_record['quarterly_return']
            
            # Map VNI returns to quarters, use 0 for missing data
            returns_df['VNI_Return'] = returns_df['Quarter_Label'].map(vni_return_map).fillna(0)
            
            # Set first VNI return to 0 to start cumulative returns from baseline
            if len(returns_df) > 0:
                returns_df.iloc[0, returns_df.columns.get_loc('VNI_Return')] = 0
            
            # Calculate cumulative returns
            returns_df['Strategy_Cumulative'] = (1 + returns_df['Strategy_Return']/100).cumprod()
            returns_df['Equal_Weight_Cumulative'] = (1 + returns_df['Equal_Weight_Return']/100).cumprod()
            returns_df['Concentrated_Cumulative'] = (1 + returns_df['Concentrated_Return']/100).cumprod()
            returns_df['VNI_Cumulative'] = (1 + returns_df['VNI_Return']/100).cumprod()
            
            return returns_df
        
        return None
        
    except Exception as e:
        st.error(f"Error calculating portfolio returns: {str(e)}")
        return None

def export_gas_strategy_results(returns_df, strategy_df, filename='gas_strategy_results.csv'):
    """Export gas strategy results to CSV file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, 'data', 'strategies_results', filename)
        
        if returns_df is None or returns_df.empty:
            st.warning("⚠️ No portfolio returns data to export")
            return None
            
        # Create comprehensive results DataFrame
        results_data = []
        
        # Combine strategy decisions with returns
        for _, row in returns_df.iterrows():
            quarter = row['Quarter_Label']
            
            # Get strategy decision info from strategy_df
            strategy_info = strategy_df[strategy_df['Quarter_Label'] == quarter]
            decision = row.get('Portfolio_Decision', 'Unknown')
            pow_weight = row.get('POW_Weight', 0)
            nt2_weight = row.get('NT2_Weight', 0)
            
            # Create selected stocks string without percentages
            if pow_weight == 1.0:
                diversified_stocks = "POW"
            elif nt2_weight == 1.0:
                diversified_stocks = "NT2"
            else:
                diversified_stocks = "POW, NT2"
            
            # Add Diversified Portfolio (original strategy with 20% threshold)
            results_data.append({
                'strategy_type': 'diversified',
                'quarter': quarter,
                'selected_stocks': diversified_stocks,
                'pow_weight': pow_weight,
                'nt2_weight': nt2_weight,
                'quarterly_return': row.get('Strategy_Return', 0),
                'cumulative_return': (row.get('Strategy_Cumulative', 1) - 1) * 100,
                'decision_basis': decision
            })
            
            # Add Concentrated Portfolio (100% in stock with higher contracted volume growth from previous quarter)
            concentrated_return = row.get('Concentrated_Return', 0)
            concentrated_pow_weight = row.get('Concentrated_POW_Weight', 0.5)
            concentrated_nt2_weight = row.get('Concentrated_NT2_Weight', 0.5)
            
            # Determine selected stock based on weights
            if concentrated_pow_weight == 1.0:
                concentrated_stock = "POW"
                decision_basis = "POW Higher Volume Growth"
            elif concentrated_nt2_weight == 1.0:
                concentrated_stock = "NT2"
                decision_basis = "NT2 Higher Volume Growth"
            else:
                concentrated_stock = "POW, NT2"
                decision_basis = "Equal (First Quarter or Missing Data)"
                
            results_data.append({
                'strategy_type': 'concentrated',
                'quarter': quarter,
                'selected_stocks': concentrated_stock,
                'pow_weight': concentrated_pow_weight,
                'nt2_weight': concentrated_nt2_weight,
                'quarterly_return': concentrated_return,
                'cumulative_return': (row.get('Concentrated_Cumulative', 1) - 1) * 100,
                'decision_basis': decision_basis
            })
        
        # Create DataFrame and save to CSV
        if results_data:
            results_df_export = pd.DataFrame(results_data)
            results_df_export = results_df_export.sort_values(['strategy_type', 'quarter']).reset_index(drop=True)
            results_df_export.to_csv(output_path, index=False)
            
            st.success(f"✅ Gas strategy results exported to: {output_path}")
            st.info(f"📊 Exported {len(results_df_export)} records covering {results_df_export['strategy_type'].nunique()} portfolio strategies")
            
            # Show summary
            strategy_summary = results_df_export.groupby('strategy_type').agg({
                'quarter': 'count',
                'quarterly_return': ['mean', 'std'],
                'cumulative_return': 'last'
            }).round(2)
            
            st.write("### Export Summary:")
            st.dataframe(strategy_summary)
            
            return output_path
        else:
            st.warning("⚠️ No data to export")
            return None
            
    except Exception as e:
        st.error(f"❌ Error exporting results: {e}")
        return None

def create_growth_line_chart(quarterly_df):
    """Create line chart showing YoY growth trends for POW and NT2"""
    try:
        if quarterly_df is None or quarterly_df.empty:
            return None
            
        # Filter data that has valid YoY growth data
        valid_data = quarterly_df.dropna(subset=['POW_YoY_Growth', 'NT2_YoY_Growth'])
        
        if valid_data.empty:
            return None
            
        fig = go.Figure()
        
        # Add POW YoY growth line
        fig.add_trace(
            go.Scatter(
                x=valid_data['Quarter_Label'],
                y=valid_data['POW_YoY_Growth'],
                mode='lines+markers',
                name='POW YoY Growth',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=6),
                hovertemplate='<b>POW</b><br>' +
                            'Quarter: %{x}<br>' +
                            'YoY Growth: %{y:.2f}%<br>' +
                            '<extra></extra>'
            )
        )
        
        # Add NT2 YoY growth line
        fig.add_trace(
            go.Scatter(
                x=valid_data['Quarter_Label'],
                y=valid_data['NT2_YoY_Growth'],
                mode='lines+markers',
                name='NT2 YoY Growth',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=6),
                hovertemplate='<b>NT2</b><br>' +
                            'Quarter: %{x}<br>' +
                            'YoY Growth: %{y:.2f}%<br>' +
                            '<extra></extra>'
            )
        )
        
        # Add horizontal line at 0%
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        fig.update_layout(
            title='POW vs NT2 YoY Growth Trends',
            xaxis_title='Quarter',
            yaxis_title='YoY Growth Rate (%)',
            hovermode='x unified',
            template='plotly_white',
            height=500,
            showlegend=True
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating growth line chart: {e}")
        return None

def plot_contracted_volume_growth(quarterly_df):
    """Plot contracted volume growth for POW and NT2"""
    try:
        if quarterly_df is None or quarterly_df.empty:
            return None
            
        # Filter data that has both POW and NT2 contracted volumes
        valid_data = quarterly_df.dropna(subset=['POW_Contracted', 'NT2_Contracted'])
        
        if valid_data.empty:
            return None
            
        fig = go.Figure()
        
        # Add POW contracted volume bars
        fig.add_trace(
            go.Bar(
                x=valid_data['Quarter_Label'],
                y=valid_data['POW_Contracted'],
                name='POW Contracted Volume',
                marker_color='#1f77b4',
                hovertemplate='<b>POW</b><br>' +
                            'Quarter: %{x}<br>' +
                            'Contracted Volume: %{y:,.0f}<br>' +
                            '<extra></extra>'
            )
        )
        
        # Add NT2 contracted volume bars
        fig.add_trace(
            go.Bar(
                x=valid_data['Quarter_Label'],
                y=valid_data['NT2_Contracted'],
                name='NT2 Contracted Volume',
                marker_color='#ff7f0e',
                hovertemplate='<b>NT2</b><br>' +
                            'Quarter: %{x}<br>' +
                            'Contracted Volume: %{y:,.0f}<br>' +
                            '<extra></extra>'
            )
        )
        
        fig.update_layout(
            title='POW vs NT2 Contracted Volume Growth',
            xaxis_title='Quarter',
            yaxis_title='Contracted Volume',
            barmode='group',
            hovermode='x unified',
            template='plotly_white',
            height=600
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Error plotting contracted volume growth: {e}")
        return None

def plot_portfolio_performance(returns_df):
    """Plot portfolio performance comparison only (no allocation chart)"""
    try:
        if returns_df is None or returns_df.empty:
            return None
            
        fig = go.Figure()
        
        # Cumulative returns starting from 0%
        fig.add_trace(
            go.Scatter(
                x=returns_df['Quarter_Label'],
                y=(returns_df['Strategy_Cumulative'] - 1) * 100,  # Convert to percentage starting from 0%
                mode='lines+markers',
                name='Strategy Portfolio',
                line=dict(color='red', width=3),
                hovertemplate='%{x}<br>Strategy: %{y:.2f}%<extra></extra>'
            )
        )
        
        fig.add_trace(
            go.Scatter(
                x=returns_df['Quarter_Label'],
                y=(returns_df['Equal_Weight_Cumulative'] - 1) * 100,  # Convert to percentage starting from 0%
                mode='lines+markers',
                name='Equal Weight',
                line=dict(color='blue', width=2),
                hovertemplate='%{x}<br>Equal Weight: %{y:.2f}%<extra></extra>'
            )
        )
        
        # Add Concentrated Portfolio (100% in stock with higher volume growth)
        fig.add_trace(
            go.Scatter(
                x=returns_df['Quarter_Label'],
                y=(returns_df['Concentrated_Cumulative'] - 1) * 100,  # Convert to percentage starting from 0%
                mode='lines+markers',
                name='Concentrated (100%)',
                line=dict(color='purple', width=2, dash='dash'),
                hovertemplate='%{x}<br>Concentrated: %{y:.2f}%<extra></extra>'
            )
        )
        
        fig.add_trace(
            go.Scatter(
                x=returns_df['Quarter_Label'],
                y=(returns_df['VNI_Cumulative'] - 1) * 100,  # Convert to percentage starting from 0%
                mode='lines+markers',
                name='VNI Index',
                line=dict(color='green', width=2),
                hovertemplate='%{x}<br>VNI: %{y:.2f}%<extra></extra>'
            )
        )
        
        # Update layout
        fig.update_layout(
            title='POW vs NT2 Portfolio Performance Comparison',
            xaxis_title='Quarter',
            yaxis_title='Cumulative Return (%)',
            height=600,
            showlegend=True,
            hovermode='x unified',
            template='plotly_white'
        )
        
        # Add horizontal line at 0%
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        return fig
        
    except Exception as e:
        st.error(f"Error plotting portfolio performance: {str(e)}")
        return None

def run_gas_strategy(pow_df=None, convert_to_excel=None, convert_to_csv=None, tab_focus=None):
    """Main function to run the gas strategy analysis"""
    try:
        pvpower_df = load_pvpower_data()
        quarterly_df = process_quarterly_data(pvpower_df)
        
        # Step 3: Construct portfolio strategy
        strategy_df = construct_portfolio_strategy(quarterly_df)
        
        if strategy_df is None:
            st.error("Could not construct portfolio strategy")
            return
            
        # Step 4: Get stock returns using SSI API (start from 2019)
        stock_data = get_stock_returns_ssi(['POW', 'NT2'], start_year=2019, end_year=2025)
        
        if not stock_data:
            st.error("Could not get stock returns data")
            return
            
        # Step 5: Calculate portfolio returns
        returns_df = calculate_portfolio_returns(strategy_df, stock_data)
        
        if returns_df is None:
            st.error("Could not calculate portfolio returns")
            return
            
        # Export strategy results to CSV
        export_gas_strategy_results(returns_df, strategy_df)
        
        # Tab-specific content display

        if tab_focus == "performance" or tab_focus is None:
            # Performance Chart Tab - Show only the chart
            
            st.write("📈 **Portfolio Performance Comparison**")
            fig = plot_portfolio_performance(returns_df)
            
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
        
        elif tab_focus == "details":
            # Portfolio Details Tab - Show two tables: stock weights and returns
            st.write("📋 **Portfolio Return Analysis**")
            
            # Table 1: Stock Weights for Each Period
            st.write("**Stock Weights by Quarter:**")
            portfolio_weights = strategy_df[['Quarter_Label', 'POW_Weight', 'NT2_Weight']].copy()
            portfolio_weights['POW_Weight'] = (portfolio_weights['POW_Weight'] * 100).round(1)
            portfolio_weights['NT2_Weight'] = (portfolio_weights['NT2_Weight'] * 100).round(1)
            portfolio_weights.columns = ['Quarter', 'POW Weight (%)', 'NT2 Weight (%)']
            st.dataframe(portfolio_weights, use_container_width=True)
            
            # Table 2: Portfolio Returns Comparison (Strategy vs Equal Weight only)
            st.write("**Portfolio Returns by Quarter:**")
            returns_comparison = returns_df[['Quarter_Label', 'Strategy_Return', 'Equal_Weight_Return']].copy()
            returns_comparison['Strategy_Return'] = returns_comparison['Strategy_Return'].round(2)
            returns_comparison['Equal_Weight_Return'] = returns_comparison['Equal_Weight_Return'].round(2)
            returns_comparison.columns = ['Quarter', 'Strategy Portfolio (%)', 'Equal Weight Portfolio (%)']
            st.dataframe(returns_comparison, use_container_width=True)
            
        elif tab_focus == "growth":
            # Volume Growth Tab - Show only the table
            st.write("📈 **Contracted Volume Growth Analysis**")
            
            # Display growth data table only
            growth_data = quarterly_df[['Quarter_Label', 'POW_Contracted', 'NT2_Contracted', 'POW_YoY_Growth', 'NT2_YoY_Growth']].copy()
            growth_data['POW_YoY_Growth'] = growth_data['POW_YoY_Growth'].round(2)
            growth_data['NT2_YoY_Growth'] = growth_data['NT2_YoY_Growth'].round(2)
            growth_data.columns = ['Quarter', 'POW Contracted Volume', 'NT2 Contracted Volume', 'POW YoY Growth (%)', 'NT2 YoY Growth (%)']
            
            st.dataframe(growth_data, use_container_width=True)
        
        # Download options (show in all tabs)
        if convert_to_excel and convert_to_csv and tab_focus != "growth":
            st.write("📥 **Download Data:**")
            
            col1, col2 = st.columns(2)
            
            # Make keys unique based on tab_focus to avoid duplicates
            tab_suffix = f"_{tab_focus}" if tab_focus else "_default"
            
            with col1:
                if st.download_button(
                    label="📊 Download Strategy Data (Excel)",
                    data=convert_to_excel(returns_df),
                    file_name="pow_nt2_strategy_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"gas_strategy_excel_download{tab_suffix}"
                ):
                    st.success("Strategy data downloaded!")
            
            with col2:
                if st.download_button(
                    label="📄 Download Strategy Data (CSV)",
                    data=convert_to_csv(returns_df),
                    file_name="pow_nt2_strategy_data.csv",
                    mime="text/csv",
                    key=f"gas_strategy_csv_download{tab_suffix}"
                ):
                    st.success("Strategy data downloaded!")
        
    except Exception as e:
        st.error(f"Error running gas strategy: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    run_gas_strategy()
