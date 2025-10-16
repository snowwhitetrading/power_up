"""
Clean Trading Strategies Module
Comprehensive implementation of all power sector trading strategies
"""

import pandas as pd
import numpy as np
import os
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_vni_data():
    """Load VNI data from vn_index_monthly.csv file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vni_file = os.path.join(script_dir, 'data',  'vn_index_monthly.csv')
        
        if os.path.exists(vni_file):
            df = pd.read_csv(vni_file)
            
            # Clean column names - the CSV has 'date' and 'VNINDEX' columns
            if len(df.columns) >= 2:
                df.columns = ['date', 'VNINDEX']  # Standardize column names
            
            # Remove any invalid rows
            df = df.dropna(subset=['date', 'VNINDEX'])
            df = df[~df['date'].astype(str).str.lower().isin(['date', 'period', 'time'])]
            
            # Clean VNINDEX values (remove commas and convert to float)
            df['VNINDEX'] = df['VNINDEX'].astype(str).str.replace(',', '')
            df['VNINDEX'] = pd.to_numeric(df['VNINDEX'], errors='coerce')
            df = df.dropna(subset=['VNINDEX'])
            
            # Convert date column - handle formats like "1Q2011", "2Q2011", etc.
            def convert_quarter_to_date(quarter_str):
                try:
                    if 'Q' in str(quarter_str):
                        quarter = int(quarter_str[0])
                        year = int(quarter_str[2:])
                        # Convert to end of quarter date for proper alignment
                        month = quarter * 3  # End of quarter month (3, 6, 9, 12)
                        return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                    else:
                        return pd.to_datetime(quarter_str)
                except:
                    return pd.NaT
            
            df['date'] = df['date'].apply(convert_quarter_to_date)
            df = df.dropna(subset=['date'])
            df = df.sort_values('date')  # Sort by date
            df.set_index('date', inplace=True)
            
            # Calculate quarterly returns
            df['Quarter_Return'] = df['VNINDEX'].pct_change() * 100
            df['Cumulative_Return'] = (1 + df['Quarter_Return']/100).cumprod() * 100 - 100
            
            # Fill first quarter return with 0
            df['Quarter_Return'].fillna(0, inplace=True)
            df['Cumulative_Return'].fillna(0, inplace=True)
            
            st.info(f"✅ Loaded VNI data: {len(df)} quarters from {df.index.min().strftime('%Y-%m')} to {df.index.max().strftime('%Y-%m')}")
            
            return df
        else:
            st.error("VNI data file (vn_index_monthly.csv) not found")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading VNI data: {e}")
        return pd.DataFrame()

def load_enso_data():
    """Load ENSO/ONI data from enso_data_quarterly.csv file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        enso_file = os.path.join(script_dir, 'data',  'enso_data_quarterly.csv')
        
        if os.path.exists(enso_file):
            df = pd.read_csv(enso_file)
            
            # Convert date column - handle formats like "1Q11", "2Q11", etc.
            def convert_quarter_to_date(quarter_str):
                try:
                    if 'Q' in str(quarter_str):
                        quarter = int(quarter_str[0])
                        year_str = quarter_str[2:]
                        year = int(f"20{year_str}") if len(year_str) == 2 else int(year_str)
                        # Use quarter-end dates to match the rest of the code
                        month = quarter * 3  # Q1=3, Q2=6, Q3=9, Q4=12
                        # Create date at end of quarter month
                        return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                    else:
                        return pd.to_datetime(quarter_str)
                except:
                    return pd.NaT
            
            df['date'] = df['date'].apply(convert_quarter_to_date)
            df = df.dropna(subset=['date'])
            df.set_index('date', inplace=True)
            
            return df
        else:
            st.error("ENSO data file (enso_data_quarterly.csv) not found")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading ENSO data: {e}")
        return pd.DataFrame()
        
        # Convert date column - handle quarterly format like "1Q2011"
        if vni_df[date_col].dtype == 'object' and 'Q' in str(vni_df[date_col].iloc[0]):
            # Convert quarterly format to date
            def quarter_to_date(quarter_str):
                try:
                    q, year = quarter_str.split('Q')
                    month = int(q) * 3 - 2  # 1Q->1, 2Q->4, 3Q->7, 4Q->10
                    return pd.to_datetime(f"{year}-{month:02d}-01")
                except:
                    return pd.to_datetime(quarter_str)
            
            vni_df['Date'] = vni_df[date_col].apply(quarter_to_date)
        else:
            vni_df['Date'] = pd.to_datetime(vni_df[date_col])
        
        # Filter from 1Q2011
        start_date = pd.to_datetime('2011-01-01')
        vni_df = vni_df[vni_df['Date'] >= start_date].copy()
        
        # Calculate quarterly returns
        vni_df = vni_df.sort_values('Date')
        vni_df['Quarter_Return'] = vni_df[value_col].pct_change() * 100
        vni_df['Quarter_Return'] = vni_df['Quarter_Return'].fillna(0)
        vni_df['Cumulative_Return'] = (1 + vni_df['Quarter_Return']/100).cumprod() * 100 - 100
        
        # Set Date as index for easier manipulation
        vni_df.set_index('Date', inplace=True)
        
        return vni_df
        
    except Exception as e:
        error_msg = f"Error loading VNI data: {e}"
        print(error_msg)
        try:
            st.error(error_msg)
        except:
            pass
        # Return empty DataFrame instead of mock data - VNI should use real data only
        return pd.DataFrame()

def get_all_power_stocks():
    """Get all power stocks from hydro, gas, and coal strategies"""
    all_stocks = set()
    
    # Get hydro stocks from water_list.csv
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        water_list_file = os.path.join(script_dir, 'data',  'water_list.csv')
        if os.path.exists(water_list_file):
            lake_df = pd.read_csv(water_list_file)
            for col in lake_df.columns:
                stocks = lake_df[col].dropna().astype(str)
                for stock in stocks:
                    stock = stock.strip()
                    if len(stock) == 3 and stock.isalpha() and stock.isupper():
                        all_stocks.add(stock)
    except Exception as e:
        st.warning(f"Could not load hydro stocks: {e}")
    
    # Add comprehensive hydro stocks list
    hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
    all_stocks.update(hydro_stocks)
    
    # Add known gas stocks
    gas_stocks = ['POW', 'NT2']
    all_stocks.update(gas_stocks)
    
    # Add known coal stocks  
    coal_stocks = ['QTP', 'PPC', 'HND']
    all_stocks.update(coal_stocks)
    
    st.info(f"Found {len(all_stocks)} power stocks: {sorted(list(all_stocks))}")
    return sorted(list(all_stocks))

def get_equal_weighted_portfolio_return():
    """Get sector-weighted portfolio return: 50% Hydro / 25% Gas / 25% Coal using raw_stock_price.csv"""
    try:
        # Define sector stocks and weights
        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
        gas_stocks = ['POW', 'NT2']
        coal_stocks = ['QTP', 'PPC', 'HND']
        
        # Sector weights: 50% Hydro, 25% Gas, 25% Coal
        sector_weights = {'hydro': 0.5, 'gas': 0.25, 'coal': 0.25}
        
        # Load data from CSV file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data', 'raw_stock_price.csv')
        
        if not os.path.exists(csv_file):
            st.error(f"❌ CSV file not found: {csv_file}")
            return pd.DataFrame()
        
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter by date range (2011 onwards)
        start_date = pd.to_datetime('2011-01-01')
        df = df[df['timestamp'] >= start_date].copy()
        
        # Create sector-based quarterly returns
        sector_returns = {'hydro': {}, 'gas': {}, 'coal': {}}
        
        # Process each stock by sector
        all_symbols = hydro_stocks + gas_stocks + coal_stocks
        
        for symbol in all_symbols:
            symbol_data = df[df['symbol'] == symbol].copy()
            
            if not symbol_data.empty:
                # Sort by date and set as index for resampling
                symbol_data = symbol_data.sort_values('timestamp')
                symbol_data = symbol_data.set_index('timestamp')
                
                # Resample to quarterly using last price of quarter
                quarterly_data = symbol_data['close'].resample('Q').last().to_frame()
                quarterly_data = quarterly_data.reset_index()
                
                # Create period labels
                quarterly_data['Year'] = quarterly_data['timestamp'].dt.year
                quarterly_data['Quarter'] = quarterly_data['timestamp'].dt.quarter
                quarterly_data['period'] = quarterly_data['Year'].astype(str) + 'Q' + quarterly_data['Quarter'].astype(str)
                
                # Calculate quarterly returns
                quarterly_data['quarterly_return'] = quarterly_data['close'].pct_change() * 100
                quarterly_data['quarterly_return'] = quarterly_data['quarterly_return'].fillna(0)
                
                # Determine sector
                if symbol in hydro_stocks:
                    sector = 'hydro'
                elif symbol in gas_stocks:
                    sector = 'gas'
                elif symbol in coal_stocks:
                    sector = 'coal'
                else:
                    continue
                
                # Store returns by period
                for _, row in quarterly_data.iterrows():
                    period = row['period']
                    if period not in sector_returns[sector]:
                        sector_returns[sector][period] = {}
                    sector_returns[sector][period][symbol] = row['quarterly_return']
        
        # Calculate sector-weighted portfolio returns
        result_data = []
        all_periods = set()
        for sector in sector_returns:
            all_periods.update(sector_returns[sector].keys())
        
        for period in sorted(all_periods):
            sector_weighted_return = 0
            
            for sector_name, weight in sector_weights.items():
                if period in sector_returns[sector_name]:
                    period_data = sector_returns[sector_name][period]
                    if period_data:
                        # Calculate equal weight return within sector
                        sector_return = sum(period_data.values()) / len(period_data)
                        # Apply sector weight
                        sector_weighted_return += weight * sector_return
            
            result_data.append({
                'period': period,
                'Quarter_Return': sector_weighted_return
            })
        
        if result_data:
            result_df = pd.DataFrame(result_data)
            
            # Convert period to date
            def period_to_date(period_str):
                try:
                    year = int(period_str[:4])
                    quarter = int(period_str[5])
                    month = quarter * 3
                    return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                except:
                    return pd.to_datetime('2011-01-01')
            
            result_df['date'] = result_df['period'].apply(period_to_date)
            result_df = result_df.set_index('date')
            result_df = result_df.sort_index()
            
            # Calculate cumulative returns
            result_df['Cumulative_Return'] = (1 + result_df['Quarter_Return']/100).cumprod() * 100 - 100
            
            st.success(f"✅ Loaded sector-weighted portfolio (50% Hydro, 25% Gas, 25% Coal) from CSV with {len(result_df)} quarters")
            return result_df
        else:
            st.error("No data available for equally weighted portfolio")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error creating equally weighted portfolio: {e}")
        return pd.DataFrame()
        
        # Fallback: create conservative baseline using VNI data
        try:
            vni_data = load_vni_data()
            if not vni_data.empty and 'Quarter_Return' in vni_data.columns:
                # Use VNI returns as baseline but make more conservative
                date_range = pd.date_range('2011-01-01', '2025-09-30', freq='QE')
                result_df = pd.DataFrame(index=date_range)
                
                # Map VNI returns and make them more conservative for equal weight
                vni_aligned = vni_data.reindex(date_range)['Quarter_Return'].fillna(0) * 0.8
                result_df['Quarter_Return'] = vni_aligned
                result_df['Cumulative_Return'] = (1 + result_df['Quarter_Return']/100).cumprod() * 100 - 100
                return result_df
        except:
            pass
        
        # If no real data available, return empty DataFrame
        st.error("No real equal weighted portfolio data found")
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Error calculating equal-weighted portfolio: {e}")
        return pd.DataFrame()

def get_hydro_flood_portfolio_return():
    """Get hydro flood level portfolio return using hydro_strategy module - flood level cumulative returns"""
    try:
        # Import hydro_strategy module to get flood level portfolio directly
        from hydro_strategy import (
            load_water_reservoir_data, 
            load_stock_mappings, 
            get_quarterly_returns,
            calculate_quarterly_growth_data,
            create_portfolios
        )
        
        # Load hydro data and create portfolios
        reservoir_df = load_water_reservoir_data()
        mappings_result = load_stock_mappings()
        
        if isinstance(mappings_result, tuple):
            mappings, liquid_stocks, illiquid_stocks = mappings_result
        else:
            mappings = mappings_result
        
        quarterly_returns = get_quarterly_returns(mappings)
        growth_data = calculate_quarterly_growth_data(reservoir_df, mappings)
        portfolios = create_portfolios(growth_data, quarterly_returns)
        
        # Get the flood level portfolio specifically - use cumulative returns directly
        if isinstance(portfolios, dict) and 'flood_level' in portfolios:
            flood_level_df = portfolios['flood_level']
            
            # Use the cumulative returns directly from the flood level portfolio
            if hasattr(flood_level_df, 'empty') and not flood_level_df.empty:
                # Return the flood level portfolio with cumulative returns
                result_df = flood_level_df.copy()
                
                # Ensure we have the right column names for trading strategies
                if 'cumulative_return' in result_df.columns:
                    result_df['Cumulative_Return'] = result_df['cumulative_return']
                if 'quarterly_return' in result_df.columns:
                    result_df['Quarter_Return'] = result_df['quarterly_return']
                    
                # Convert period to datetime index if needed
                if 'period' in result_df.columns:
                    def period_to_date(period_str):
                        try:
                            if 'Q' in str(period_str):
                                year = int(period_str[:4])
                                quarter = int(period_str[5])
                                month = quarter * 3  # End of quarter month
                                return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                            return pd.to_datetime(period_str)
                        except:
                            return pd.to_datetime('2011-01-01')
                    
                    result_df['Date'] = result_df['period'].apply(period_to_date)
                    result_df = result_df.set_index('Date')
                
                st.success(f"✅ Loaded hydro flood level portfolio with {len(result_df)} periods")
                return result_df
        
        # If no flood level portfolio data, return empty DataFrame
        st.warning("No hydro flood level portfolio data available from strategy module")
        return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading hydro flood portfolio: {e}")
        return pd.DataFrame()

def get_gas_contracted_volume_return():
    """Get gas contracted volume portfolio return using gas_strategy module - concentrated portfolio (Best Growth) returns"""
    try:
        # Import gas_strategy module to get concentrated portfolio (Best Growth) directly
        from gas_strategy import (
            load_pvpower_data,
            process_quarterly_data,
            construct_portfolio_strategy,
            get_stock_returns_ssi,
            calculate_portfolio_returns
        )
        
        # Run the gas strategy to get portfolio returns
        pvpower_df = load_pvpower_data()
        if pvpower_df is None or pvpower_df.empty:
            st.warning("No PVPower data available for gas strategy")
            return pd.DataFrame()
        
        # Process quarterly data
        quarterly_df = process_quarterly_data(pvpower_df)
        if quarterly_df is None or quarterly_df.empty:
            st.warning("No quarterly data available for gas strategy")
            return pd.DataFrame()
        
        # Construct portfolio strategy
        strategy_df = construct_portfolio_strategy(quarterly_df)
        if strategy_df is None or strategy_df.empty:
            st.warning("No strategy data available for gas strategy")
            return pd.DataFrame()
        
        # Get stock returns
        gas_stocks = ['POW', 'NT2']
        try:
            stock_data = get_stock_returns_ssi(gas_stocks, start_year=2019, end_year=2025)
        except Exception as e:
            st.warning(f"Error getting stock data from gas strategy: {e}")
            return pd.DataFrame()
        
        if not stock_data:
            st.warning("No stock data available for gas strategy")
            return pd.DataFrame()
        
        # Calculate portfolio returns (this includes Best_Growth_Return which is our concentrated portfolio)
        returns_df = calculate_portfolio_returns(strategy_df, stock_data)
        
        if returns_df is not None and not returns_df.empty and 'Best_Growth_Return' in returns_df.columns:
            # Use the Best Growth (concentrated) portfolio returns directly
            result_df = returns_df[['Quarter_Label', 'Best_Growth_Return', 'Best_Growth_Cumulative']].copy()
            result_df = result_df.rename(columns={
                'Quarter_Label': 'period',
                'Best_Growth_Return': 'quarterly_return', 
                'Best_Growth_Cumulative': 'cumulative_return'
            })
            
            # Ensure we have the right column names for trading strategies
            result_df['Quarter_Return'] = result_df['quarterly_return']
            result_df['Cumulative_Return'] = (result_df['cumulative_return'] - 1) * 100  # Convert to percentage
            
            # Convert period to datetime index if needed
            if 'period' in result_df.columns:
                def period_to_date(period_str):
                    try:
                        if 'Q' in str(period_str):
                            year = int(period_str[:4])
                            quarter = int(period_str[5])
                            month = quarter * 3  # End of quarter month
                            return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                        return pd.to_datetime(period_str)
                    except:
                        return pd.to_datetime('2019-01-01')
                
                result_df['Date'] = result_df['period'].apply(period_to_date)
                result_df = result_df.set_index('Date')
            
            st.success(f"✅ Loaded gas concentrated portfolio (Best Growth) with {len(result_df)} periods")
            return result_df
        
        # If no concentrated portfolio data, return empty DataFrame
        st.warning("No gas concentrated portfolio data available from strategy module")
        return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading gas concentrated portfolio: {e}")
        return pd.DataFrame()

def get_coal_volume_growth_return():
    """Get coal volume growth return using coal_strategy module - concentrated portfolio returns"""
    try:
        # Import coal_strategy module to get concentrated portfolio directly
        from coal_strategy import (
            load_coal_volume_data,
            calculate_yoy_growth,
            fetch_stock_data,
            convert_to_quarterly_returns,
            create_coal_portfolios,
            calculate_cumulative_returns
        )
        
        # Run the coal strategy to get portfolios
        coal_df = load_coal_volume_data()
        if coal_df.empty:
            st.warning("No coal volume data available")
            return pd.DataFrame()
        
        # Calculate growth and get stock data
        growth_data = calculate_yoy_growth(coal_df)
        coal_stocks = ['PPC', 'QTP', 'HND']
        stock_data = fetch_stock_data(coal_stocks)
        
        if not stock_data:
            st.warning("No stock data available for coal strategy")
            return pd.DataFrame()
        
        # Convert to quarterly returns and create portfolios
        quarterly_returns = convert_to_quarterly_returns(stock_data)
        portfolios = create_coal_portfolios(growth_data, quarterly_returns)
        
        # Get the CONCENTRATED portfolio specifically (portfolios['concentrated'])
        if portfolios and 'concentrated' in portfolios:
            concentrated_portfolio_data = portfolios['concentrated']
            
            # Calculate cumulative returns for concentrated portfolio
            concentrated_df = calculate_cumulative_returns(concentrated_portfolio_data, start_period='2018Q4')
            
            if not concentrated_df.empty and hasattr(concentrated_df, 'columns'):
                # Use the concentrated portfolio cumulative returns directly
                result_df = concentrated_df.copy()
                
                # Ensure we have the right column names for trading strategies
                if 'cumulative_return' in result_df.columns:
                    result_df['Cumulative_Return'] = result_df['cumulative_return']
                if 'quarterly_return' in result_df.columns:
                    result_df['Quarter_Return'] = result_df['quarterly_return']
                    
                # Convert period to datetime index if needed
                if 'period' in result_df.columns:
                    def period_to_date(period_str):
                        try:
                            if 'Q' in str(period_str):
                                year = int(period_str[:4])
                                quarter = int(period_str[5])
                                month = quarter * 3  # End of quarter month
                                return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                            return pd.to_datetime(period_str)
                        except:
                            return pd.to_datetime('2019-01-01')
                    
                    result_df['Date'] = result_df['period'].apply(period_to_date)
                    result_df = result_df.set_index('Date')
                
                st.success(f"✅ Loaded coal concentrated portfolio with {len(result_df)} periods")
                return result_df
        
        # If no concentrated portfolio data, return empty DataFrame
        st.warning("No coal concentrated portfolio data available from strategy module")
        return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading coal concentrated portfolio: {e}")
        return pd.DataFrame()

def calculate_oni_based_strategy(enso_df=None):
    """Calculate ONI-based strategy using equal weighted portfolios for each sector from raw_stock_price.csv"""
    try:
        # Load ENSO data if not provided
        if enso_df is None or enso_df.empty:
            enso_df = load_enso_data()
        
        if enso_df.empty:
            st.error("No ENSO data available for ONI strategy")
            return pd.DataFrame()
        
        # Get equal weighted portfolio for each sector
        st.info("Loading equal weighted portfolio data for ONI strategy from CSV...")
        
        # Define sector stocks for equal weighted portfolios
        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
        gas_stocks = ['POW', 'NT2']
        coal_stocks = ['QTP', 'PPC', 'HND']
        
        # Load data from CSV file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data', 'raw_stock_price.csv')
        
        if not os.path.exists(csv_file):
            st.error(f"❌ CSV file not found: {csv_file}")
            return pd.DataFrame()
        
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter by date range (2011 onwards)
        start_date = pd.to_datetime('2011-01-01')
        df = df[df['timestamp'] >= start_date].copy()
        
        # Calculate returns for each sector
        sector_returns = {}
        
        for sector_name, stocks in [('hydro', hydro_stocks), ('gas', gas_stocks), ('coal', coal_stocks)]:
            quarterly_returns = {}
            
            for symbol in stocks:
                symbol_data = df[df['symbol'] == symbol].copy()
                
                if not symbol_data.empty:
                    # Sort by date and set as index for resampling
                    symbol_data = symbol_data.sort_values('timestamp')
                    symbol_data = symbol_data.set_index('timestamp')
                    
                    # Resample to quarterly using last price of quarter
                    quarterly_data = symbol_data['close'].resample('Q').last().to_frame()
                    quarterly_data = quarterly_data.reset_index()
                    
                    # Create period labels
                    quarterly_data['Year'] = quarterly_data['timestamp'].dt.year
                    quarterly_data['Quarter'] = quarterly_data['timestamp'].dt.quarter
                    quarterly_data['period'] = quarterly_data['Year'].astype(str) + 'Q' + quarterly_data['Quarter'].astype(str)
                    
                    # Calculate quarterly returns
                    quarterly_data['quarterly_return'] = quarterly_data['close'].pct_change() * 100
                    quarterly_data['quarterly_return'] = quarterly_data['quarterly_return'].fillna(0)
                    
                    # Store returns by period
                    for _, row in quarterly_data.iterrows():
                        period = row['period']
                        if period not in quarterly_returns:
                            quarterly_returns[period] = {}
                        quarterly_returns[period][symbol] = row['quarterly_return']
            
            # Calculate equal weighted returns for this sector
            sector_data = []
            for period in sorted(quarterly_returns.keys()):
                period_data = quarterly_returns[period]
                if period_data:
                    equal_return = sum(period_data.values()) / len(period_data)
                    sector_data.append({
                        'period': period,
                        'Quarter_Return': equal_return
                    })
            
            if sector_data:
                sector_df = pd.DataFrame(sector_data)
                
                def period_to_date(period_str):
                    try:
                        year = int(period_str[:4])
                        quarter = int(period_str[5])
                        month = quarter * 3
                        return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                    except:
                        return pd.to_datetime('2011-01-01')
                
                sector_df['date'] = sector_df['period'].apply(period_to_date)
                sector_df = sector_df.set_index('date')
                sector_returns[sector_name] = sector_df
        
        # Create date range
        date_range = pd.date_range('2011-01-01', '2025-09-30', freq='QE')
        result_df = pd.DataFrame(index=date_range)
        
        # Calculate ONI strategy returns
        returns = []
        for date in date_range:
            # Get ONI value for this quarter
            oni_val = 0
            year = date.year
            quarter = (date.month - 1) // 3 + 1
            
            # Find matching ONI value
            oni_found = False
            for enso_date, row in enso_df.iterrows():
                enso_year = enso_date.year
                enso_quarter = (enso_date.month - 1) // 3 + 1
                
                if enso_year == year and enso_quarter == quarter:
                    oni_val = row['ONI'] if 'ONI' in row else 0
                    oni_found = True
                    break
            
            if not oni_found:
                closest_date = min(enso_df.index, key=lambda x: abs((x - date).days))
                if abs((closest_date - date).days) < 100:
                    oni_val = enso_df.loc[closest_date, 'ONI'] if 'ONI' in enso_df.columns else 0
            
            # Get sector returns for this date
            hydro_ret = 0
            gas_ret = 0
            coal_ret = 0
            
            for sector_name, sector_data in sector_returns.items():
                if not sector_data.empty:
                    if date in sector_data.index:
                        ret = sector_data.loc[date, 'Quarter_Return']
                    else:
                        # Find closest date
                        closest_sector_date = min(sector_data.index, key=lambda x: abs((x - date).days))
                        if abs((closest_sector_date - date).days) < 100:
                            ret = sector_data.loc[closest_sector_date, 'Quarter_Return']
                        else:
                            ret = 0
                    
                    if sector_name == 'hydro':
                        hydro_ret = ret
                    elif sector_name == 'gas':
                        gas_ret = ret
                    elif sector_name == 'coal':
                        coal_ret = ret
            
            # Apply ONI-based allocation strategy
            if oni_val > 0.5:
                # ONI > 0.5: invest 50%/50% in coal/gas equally
                weighted_return = 0.5 * gas_ret + 0.5 * coal_ret
            elif oni_val < -0.5:
                # ONI < -0.5: invest 100% in hydro equal weighted portfolio
                weighted_return = hydro_ret
            else:
                # -0.5 <= ONI <= 0.5: invest 50%/25%/25% in hydro/coal/gas
                weighted_return = 0.5 * hydro_ret + 0.25 * gas_ret + 0.25 * coal_ret
            
            returns.append(weighted_return)
        
        result_df['Quarter_Return'] = returns
        result_df['Cumulative_Return'] = (1 + result_df['Quarter_Return']/100).cumprod() * 100 - 100
        
        st.success(f"✅ Calculated ONI strategy from CSV with {len(result_df)} quarters")
        return result_df
        
    except Exception as e:
        st.error(f"Error calculating ONI-based strategy: {e}")
        return pd.DataFrame()

def calculate_alpha_strategy(enso_df=None):
    """Calculate Alpha strategy using strategy results CSV files"""
    try:
        # Load ENSO data for ONI values if not provided
        if enso_df is None or enso_df.empty:
            enso_df = load_enso_data()
        
        if enso_df.empty:
            st.error("No ENSO data available for Alpha strategy")
            return pd.DataFrame()
        
        # Load strategy results from CSV files
        st.info("Loading strategy results from CSV files for Alpha strategy...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Load hydro strategy results (flood_level portfolio)
        hydro_file = os.path.join(script_dir, 'data', 'strategies_results', 'hydro_strategy_results.csv')
        hydro_df = pd.read_csv(hydro_file)
        hydro_df = hydro_df[hydro_df['strategy_type'] == 'flood_level'].copy()
        
        # Load gas strategy results (concentrated portfolio)
        gas_file = os.path.join(script_dir, 'data', 'strategies_results', 'gas_strategy_results.csv')
        gas_df = pd.read_csv(gas_file)
        gas_df = gas_df[gas_df['strategy_type'] == 'concentrated'].copy()
        
        # Load coal strategy results (concentrated portfolio)
        coal_file = os.path.join(script_dir, 'data', 'strategies_results', 'coal_strategy_results.csv')
        coal_df = pd.read_csv(coal_file)
        coal_df = coal_df[coal_df['strategy_type'] == 'concentrated'].copy()
        
        # Convert period to datetime for matching
        def period_to_date(period_str):
            try:
                if 'Q' in str(period_str):
                    year = int(period_str[:4])
                    quarter = int(period_str[5])
                    month = quarter * 3
                    return pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
                else:
                    return pd.to_datetime(period_str)
            except:
                return pd.NaT
        
        hydro_df['date'] = hydro_df['period'].apply(period_to_date)
        gas_df['date'] = gas_df['quarter'].apply(period_to_date)
        coal_df['date'] = coal_df['period'].apply(period_to_date)
        
        # Set as index
        hydro_df = hydro_df.set_index('date')
        gas_df = gas_df.set_index('date')
        coal_df = coal_df.set_index('date')
        
        # Get equal weighted portfolio for before 1Q2019 and fallback
        equal_weighted = get_equal_weighted_portfolio_return()
        
        # Get ONI strategy for before 1Q2019
        oni_strategy = calculate_oni_based_strategy(enso_df)
        
        # Create quarterly date range from 1Q2011 to 3Q2025
        date_range = pd.date_range('2011-01-01', '2025-09-30', freq='QE')
        result_df = pd.DataFrame(index=date_range)
        
        returns = []
        for i, date in enumerate(date_range):
            # Before 1Q2019: Alpha should equal ONI
            if date < pd.to_datetime('2019-01-01'):
                # Use ONI strategy return for this period
                if not oni_strategy.empty and date in oni_strategy.index:
                    quarterly_return = oni_strategy.loc[date, 'Quarter_Return']
                else:
                    quarterly_return = 0
                returns.append(quarterly_return)
                continue
            
            # From 1Q2019 onwards: Use complex Alpha strategy with CSV data
            # Get ONI value for this quarter
            oni_val = 0
            year = date.year
            quarter = (date.month - 1) // 3 + 1
            
            # Find matching ONI value
            oni_found = False
            for enso_date, row in enso_df.iterrows():
                enso_year = enso_date.year
                enso_quarter = (enso_date.month - 1) // 3 + 1
                
                if enso_year == year and enso_quarter == quarter:
                    oni_val = row['ONI'] if 'ONI' in row else 0
                    oni_found = True
                    break
            
            if not oni_found:
                closest_date = min(enso_df.index, key=lambda x: abs((x - date).days))
                if abs((closest_date - date).days) < 100:
                    oni_val = enso_df.loc[closest_date, 'ONI'] if 'ONI' in enso_df.columns else 0
            
            # Apply Alpha strategy based on ONI conditions
            quarterly_return = 0
            
            if oni_val > 0.5:
                # ONI > 0.5: invest 50% in gas and 50% in coal
                gas_ret = 0
                coal_ret = 0
                
                # Get gas return from CSV
                if date in gas_df.index:
                    gas_ret = gas_df.loc[date, 'quarterly_return']
                else:
                    closest_gas = min(gas_df.index, key=lambda x: abs((x - date).days)) if not gas_df.empty else None
                    if closest_gas and abs((closest_gas - date).days) < 100:
                        gas_ret = gas_df.loc[closest_gas, 'quarterly_return']
                
                # Get coal return from CSV
                if date in coal_df.index:
                    coal_ret = coal_df.loc[date, 'quarterly_return']
                else:
                    closest_coal = min(coal_df.index, key=lambda x: abs((x - date).days)) if not coal_df.empty else None
                    if closest_coal and abs((closest_coal - date).days) < 100:
                        coal_ret = coal_df.loc[closest_coal, 'quarterly_return']
                
                quarterly_return = 0.5 * gas_ret + 0.5 * coal_ret
                    
            elif oni_val < -0.5:
                # ONI < -0.5: invest 100% in hydro
                if date < pd.to_datetime('2020-04-01'):
                    # 1Q2011 to 1Q2020: use equally weighted portfolio
                    if not equal_weighted.empty and date in equal_weighted.index:
                        quarterly_return = equal_weighted.loc[date, 'Quarter_Return']
                    elif not equal_weighted.empty and i < len(equal_weighted):
                        quarterly_return = equal_weighted.iloc[i]['Quarter_Return']
                else:
                    # 2Q2020 onwards: use hydro flood level portfolio from CSV
                    if date in hydro_df.index:
                        quarterly_return = hydro_df.loc[date, 'quarterly_return']
                    else:
                        closest_hydro = min(hydro_df.index, key=lambda x: abs((x - date).days)) if not hydro_df.empty else None
                        if closest_hydro and abs((closest_hydro - date).days) < 100:
                            quarterly_return = hydro_df.loc[closest_hydro, 'quarterly_return']
                        
            else:
                # -0.5 <= ONI <= 0.5: invest 50%/25%/25% in hydro/gas/coal
                hydro_ret = 0
                gas_ret = 0
                coal_ret = 0
                
                # Get gas return from CSV
                if date in gas_df.index:
                    gas_ret = gas_df.loc[date, 'quarterly_return']
                else:
                    closest_gas = min(gas_df.index, key=lambda x: abs((x - date).days)) if not gas_df.empty else None
                    if closest_gas and abs((closest_gas - date).days) < 100:
                        gas_ret = gas_df.loc[closest_gas, 'quarterly_return']
                
                # Get coal return from CSV
                if date in coal_df.index:
                    coal_ret = coal_df.loc[date, 'quarterly_return']
                else:
                    closest_coal = min(coal_df.index, key=lambda x: abs((x - date).days)) if not coal_df.empty else None
                    if closest_coal and abs((closest_coal - date).days) < 100:
                        coal_ret = coal_df.loc[closest_coal, 'quarterly_return']
                
                # For hydro: use CSV data from 2Q2020, equal weighted before
                if date >= pd.to_datetime('2020-04-01'):
                    # Use hydro flood level portfolio from CSV
                    if date in hydro_df.index:
                        hydro_ret = hydro_df.loc[date, 'quarterly_return']
                    else:
                        closest_hydro = min(hydro_df.index, key=lambda x: abs((x - date).days)) if not hydro_df.empty else None
                        if closest_hydro and abs((closest_hydro - date).days) < 100:
                            hydro_ret = hydro_df.loc[closest_hydro, 'quarterly_return']
                else:
                    # Before 2Q2020, use equal weighted for hydro portion
                    if not equal_weighted.empty and date in equal_weighted.index:
                        hydro_ret = equal_weighted.loc[date, 'Quarter_Return']
                    elif not equal_weighted.empty and i < len(equal_weighted):
                        hydro_ret = equal_weighted.iloc[i]['Quarter_Return']
                
                quarterly_return = 0.5 * hydro_ret + 0.25 * gas_ret + 0.25 * coal_ret
            
            returns.append(quarterly_return)
        
        # Create result DataFrame
        result_df['Quarter_Return'] = returns
        result_df['Cumulative_Return'] = (1 + result_df['Quarter_Return']/100).cumprod() * 100 - 100
        
        st.success(f"✅ Calculated Alpha strategy from CSV files with {len(result_df)} quarters")
        return result_df
        
    except Exception as e:
        st.error(f"Error calculating Alpha strategy: {e}")
        import traceback
        st.error(traceback.format_exc())
        return pd.DataFrame()

def create_comprehensive_strategy_comparison(enso_df=None):
    """Create comprehensive comparison of all strategies using real data"""
    try:
        # Show info message if in Streamlit context
        try:
            st.info("📊 Generating comprehensive strategy comparison...")
        except:
            print("📊 Generating comprehensive strategy comparison...")
        
        # Get all strategies
        vni_data = load_vni_data()
        equal_data = get_equal_weighted_portfolio_return()
        oni_data = calculate_oni_based_strategy()
        alpha_data = calculate_alpha_strategy()
        
        # Create unified DataFrame
        date_range = pd.date_range('2011-01-01', '2025-09-30', freq='QE')
        
        unified_df = pd.DataFrame(index=date_range)
        # Convert to string format for better chart compatibility
        unified_df['Period'] = date_range.strftime('%Y-%m-%d')
        
        # Add all strategy data with proper error handling and type conversion
        if not vni_data.empty and 'Quarter_Return' in vni_data.columns:
            try:
                vni_returns = vni_data.reindex(date_range)['Quarter_Return']
                vni_cumulative = vni_data.reindex(date_range)['Cumulative_Return'] 
                
                # Force numeric conversion with proper error handling
                unified_df['VNI_Return'] = pd.to_numeric(vni_returns.astype(str), errors='coerce').fillna(0.0)
                unified_df['VNI_Cumulative'] = pd.to_numeric(vni_cumulative.astype(str), errors='coerce').fillna(0.0)
            except Exception as e:
                st.warning(f"Error processing VNI data: {e}")
                unified_df['VNI_Return'] = 0.0
                unified_df['VNI_Cumulative'] = 0.0
        else:
            unified_df['VNI_Return'] = 0.0
            unified_df['VNI_Cumulative'] = 0.0
            
        if not equal_data.empty and 'Quarter_Return' in equal_data.columns:
            try:
                equal_returns = equal_data.reindex(date_range)['Quarter_Return']
                equal_cumulative = equal_data.reindex(date_range)['Cumulative_Return']
                
                unified_df['Equal_Return'] = pd.to_numeric(equal_returns.astype(str), errors='coerce').fillna(0.0)
                unified_df['Equal_Cumulative'] = pd.to_numeric(equal_cumulative.astype(str), errors='coerce').fillna(0.0)
            except Exception as e:
                st.warning(f"Error processing Equal Weight data: {e}")
                unified_df['Equal_Return'] = 0.0
                unified_df['Equal_Cumulative'] = 0.0
        else:
            unified_df['Equal_Return'] = 0.0
            unified_df['Equal_Cumulative'] = 0.0
            
        if not oni_data.empty and 'Quarter_Return' in oni_data.columns:
            try:
                oni_returns = oni_data.reindex(date_range)['Quarter_Return']
                oni_cumulative = oni_data.reindex(date_range)['Cumulative_Return']
                
                unified_df['ONI_Return'] = pd.to_numeric(oni_returns.astype(str), errors='coerce').fillna(0.0)
                unified_df['ONI_Cumulative'] = pd.to_numeric(oni_cumulative.astype(str), errors='coerce').fillna(0.0)
            except Exception as e:
                st.warning(f"Error processing ONI data: {e}")
                unified_df['ONI_Return'] = 0.0
                unified_df['ONI_Cumulative'] = 0.0
        else:
            unified_df['ONI_Return'] = 0.0
            unified_df['ONI_Cumulative'] = 0.0
            
        if not alpha_data.empty and 'Quarter_Return' in alpha_data.columns:
            try:
                alpha_returns = alpha_data.reindex(date_range)['Quarter_Return']
                alpha_cumulative = alpha_data.reindex(date_range)['Cumulative_Return']
                
                unified_df['Alpha_Return'] = pd.to_numeric(alpha_returns.astype(str), errors='coerce').fillna(0.0)
                unified_df['Alpha_Cumulative'] = pd.to_numeric(alpha_cumulative.astype(str), errors='coerce').fillna(0.0)
            except Exception as e:
                st.warning(f"Error processing Alpha data: {e}")
                unified_df['Alpha_Return'] = 0.0
                unified_df['Alpha_Cumulative'] = 0.0
        else:
            unified_df['Alpha_Return'] = 0.0
            unified_df['Alpha_Cumulative'] = 0.0
        
        # Automatically export to CSV whenever comprehensive comparison is created
        if not unified_df.empty:
            try:
                export_strategy_comparison_to_csv(unified_df)
                # Also export stock weights
                export_stock_weights_to_csv(enso_df)
            except Exception as export_error:
                print(f"Auto-export failed: {export_error}")
        
        return unified_df
        
    except Exception as e:
        try:
            st.error(f"Error creating comprehensive strategy comparison: {e}")
        except:
            print(f"Error creating comprehensive strategy comparison: {e}")
        return pd.DataFrame()

def export_strategy_comparison_to_csv(unified_df):
    """Export the strategy comparison DataFrame to trading_strategies_comparison.csv"""
    try:
        if unified_df is None or unified_df.empty:
            print("No data to export - DataFrame is empty")
            return False
            
        # Prepare the data for export
        export_df = unified_df.copy()
        
        # Reset index to include the date as a column
        export_df = export_df.reset_index()
        if 'index' in export_df.columns:
            export_df = export_df.rename(columns={'index': 'Date'})
        
        # Ensure Period column exists
        if 'Period' not in export_df.columns and 'Date' in export_df.columns:
            export_df['Period'] = export_df['Date'].dt.strftime('%Y-%m-%d')
        
        # Reorder columns for better readability
        column_order = ['Period', 'VNI_Return', 'Equal_Return', 'ONI_Return', 'Alpha_Return',
                       'VNI_Cumulative', 'Equal_Cumulative', 'ONI_Cumulative', 'Alpha_Cumulative']
        
        # Only include columns that exist in the DataFrame
        available_columns = [col for col in column_order if col in export_df.columns]
        export_df = export_df[available_columns]
        
        # Get the script directory and create the file path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file_path = os.path.join(script_dir, 'data','strategies_results', 'trading_strategies_comparison.csv')
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        # Export to CSV
        export_df.to_csv(csv_file_path, index=False)
        
        print(f"✅ Successfully exported strategy comparison data to: {csv_file_path}")
        try:
            st.success(f"🎉 Successfully exported strategy comparison data to: trading_strategies_comparison.csv")
            st.info(f"📁 File location: {csv_file_path}")
        except:
            pass
            
        return True
        
    except Exception as e:
        error_msg = f"Error exporting strategy comparison to CSV: {e}"
        print(error_msg)
        try:
            st.error(error_msg)
        except:
            pass
        return False


def export_stock_weights_to_csv(enso_df=None):
    """Export detailed stock weights for all strategies to stock_weights.csv
    
    Columns: symbol, period, strategy, weight
    """
    try:
        print("🔄 Generating stock weights CSV...")
        
        # Load ENSO data if not provided
        if enso_df is None or enso_df.empty:
            enso_df = load_enso_data()
        
        if enso_df.empty:
            print("❌ No ENSO data available for stock weights export")
            return False
        
        # Ensure enso_df has datetime index (load_enso_data returns it with datetime index)
        # If it doesn't have datetime index, convert it
        if not isinstance(enso_df.index, pd.DatetimeIndex):
            if 'date' in enso_df.columns:
                enso_df = enso_df.set_index('date')
            enso_df.index = pd.to_datetime(enso_df.index)
        
        # Load strategy results from CSV files
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Load hydro strategy results
        hydro_file = os.path.join(script_dir, 'data', 'strategies_results', 'hydro_strategy_results.csv')
        hydro_df = pd.read_csv(hydro_file)
        
        # Load gas strategy results
        gas_file = os.path.join(script_dir, 'data', 'strategies_results', 'gas_strategy_results.csv')
        gas_df = pd.read_csv(gas_file)
        
        # Load coal strategy results
        coal_file = os.path.join(script_dir, 'data', 'strategies_results', 'coal_strategy_results.csv')
        coal_df = pd.read_csv(coal_file)
        
        # Define stock lists
        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
        gas_stocks = ['POW', 'NT2']
        coal_stocks = ['QTP', 'PPC', 'HND']
        
        # Create date range from 1Q2011 to 3Q2025
        date_range = pd.date_range('2011-01-01', '2025-09-30', freq='QE')
        
        # Storage for all weight records
        weight_records = []
        
        # Process each quarter
        for date in date_range:
            year = date.year
            quarter = (date.month - 1) // 3 + 1
            period = f"{year}Q{quarter}"
            
            # Get ONI value for this quarter
            oni_val = 0
            oni_found = False
            for enso_date, row in enso_df.iterrows():
                enso_year = enso_date.year
                enso_quarter = (enso_date.month - 1) // 3 + 1
                
                if enso_year == year and enso_quarter == quarter:
                    oni_val = row['ONI'] if 'ONI' in row else 0
                    oni_found = True
                    break
            
            if not oni_found:
                closest_date = min(enso_df.index, key=lambda x: abs((x - date).days))
                if abs((closest_date - date).days) < 100:
                    oni_val = enso_df.loc[closest_date, 'ONI'] if 'ONI' in enso_df.columns else 0
            
            # Determine ENSO condition
            if oni_val > 0.5:
                condition = "El Niño"
            elif oni_val < -0.5:
                condition = "La Niña"
            else:
                condition = "Neutral"
            
            # ===== EQUAL WEIGHTED STRATEGY =====
            # Always 50% Hydro (equal weighted), 25% Gas (equal weighted), 25% Coal (equal weighted)
            for stock in hydro_stocks:
                weight_records.append({
                    'symbol': stock,
                    'period': period,
                    'strategy': 'Equal_Weighted',
                    'weight': 50.0 / len(hydro_stocks)
                })
            
            for stock in gas_stocks:
                weight_records.append({
                    'symbol': stock,
                    'period': period,
                    'strategy': 'Equal_Weighted',
                    'weight': 25.0 / len(gas_stocks)
                })
            
            for stock in coal_stocks:
                weight_records.append({
                    'symbol': stock,
                    'period': period,
                    'strategy': 'Equal_Weighted',
                    'weight': 25.0 / len(coal_stocks)
                })
            
            # ===== ONI STRATEGY =====
            if condition == "El Niño":
                # 50% Gas + 50% Coal (equal weighted within sector)
                for stock in gas_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 50.0 / len(gas_stocks)
                    })
                
                for stock in coal_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 50.0 / len(coal_stocks)
                    })
            
            elif condition == "La Niña":
                # 100% Hydro (equal weighted)
                for stock in hydro_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 100.0 / len(hydro_stocks)
                    })
            
            else:  # Neutral
                # 50% Hydro + 25% Gas + 25% Coal (equal weighted within sector)
                for stock in hydro_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 50.0 / len(hydro_stocks)
                    })
                
                for stock in gas_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 25.0 / len(gas_stocks)
                    })
                
                for stock in coal_stocks:
                    weight_records.append({
                        'symbol': stock,
                        'period': period,
                        'strategy': 'ONI',
                        'weight': 25.0 / len(coal_stocks)
                    })
            
            # ===== ALPHA STRATEGY =====
            # Before 1Q2019: Alpha = ONI
            if date < pd.to_datetime('2019-01-01'):
                # Copy ONI weights for Alpha
                if condition == "El Niño":
                    for stock in gas_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 50.0 / len(gas_stocks)
                        })
                    
                    for stock in coal_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 50.0 / len(coal_stocks)
                        })
                
                elif condition == "La Niña":
                    for stock in hydro_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 100.0 / len(hydro_stocks)
                        })
                
                else:  # Neutral
                    for stock in hydro_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 50.0 / len(hydro_stocks)
                        })
                    
                    for stock in gas_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 25.0 / len(gas_stocks)
                        })
                    
                    for stock in coal_stocks:
                        weight_records.append({
                            'symbol': stock,
                            'period': period,
                            'strategy': 'Alpha',
                            'weight': 25.0 / len(coal_stocks)
                        })
            
            # From 1Q2019 onwards: Use specialized portfolios
            else:
                if condition == "El Niño":
                    # 50% Gas (specialized) + 50% Coal (specialized)
                    # Get gas weights from gas_strategy_results.csv
                    gas_period_data = gas_df[gas_df['quarter'] == period]
                    if not gas_period_data.empty:
                        gas_row = gas_period_data[gas_period_data['strategy_type'] == 'concentrated'].iloc[0]
                        pow_weight = gas_row['pow_weight'] if 'pow_weight' in gas_row else 0.5
                        nt2_weight = gas_row['nt2_weight'] if 'nt2_weight' in gas_row else 0.5
                        
                        # Only add stocks with non-zero weights
                        # Note: weights in CSV are in decimal (1.0 = 100%), convert to percentage and multiply by sector allocation
                        if pow_weight > 0:
                            weight_records.append({
                                'symbol': 'POW',
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': pow_weight * 100 * 0.5  # Convert to % and multiply by 50% gas allocation
                            })
                        
                        if nt2_weight > 0:
                            weight_records.append({
                                'symbol': 'NT2',
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': nt2_weight * 100 * 0.5  # Convert to % and multiply by 50% gas allocation
                            })
                    else:
                        # Fallback to equal weight
                        for stock in gas_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 50.0 / len(gas_stocks)
                            })
                    
                    # Get coal weights from coal_strategy_results.csv
                    coal_period_data = coal_df[coal_df['period'] == period]
                    if not coal_period_data.empty:
                        coal_row = coal_period_data[coal_period_data['strategy_type'] == 'concentrated'].iloc[0]
                        selected_stocks_str = coal_row['selected_stocks'] if 'selected_stocks' in coal_row else ''
                        
                        # Parse selected stocks (format: "PPC, QTP" or "QTP")
                        selected_coal = [s.strip() for s in str(selected_stocks_str).split(',') if s.strip() in coal_stocks]
                        
                        if selected_coal:
                            coal_weight_per_stock = 50.0 / len(selected_coal)
                            for stock in selected_coal:
                                weight_records.append({
                                    'symbol': stock,
                                    'period': period,
                                    'strategy': 'Alpha',
                                    'weight': coal_weight_per_stock
                                })
                    else:
                        # Fallback to equal weight
                        for stock in coal_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 50.0 / len(coal_stocks)
                            })
                
                elif condition == "La Niña":
                    # Before 2Q2020: use equal weighted (50% Hydro, 25% Gas, 25% Coal)
                    # From 2Q2020: use 100% Hydro flood level portfolio
                    if date < pd.to_datetime('2020-04-01'):
                        # Equal weighted portfolio
                        for stock in hydro_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 50.0 / len(hydro_stocks)
                            })
                        
                        for stock in gas_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 25.0 / len(gas_stocks)
                            })
                        
                        for stock in coal_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 25.0 / len(coal_stocks)
                            })
                    else:
                        # 100% Hydro flood level portfolio
                        hydro_period_data = hydro_df[hydro_df['period'] == period]
                        if not hydro_period_data.empty:
                            hydro_row = hydro_period_data[hydro_period_data['strategy_type'] == 'flood_level'].iloc[0]
                            liquid_stocks_str = hydro_row['liquid_stock'] if 'liquid_stock' in hydro_row else ''
                            illiquid_stocks_str = hydro_row['illiquid_stock'] if 'illiquid_stock' in hydro_row else ''
                            
                            # Parse stocks - include ALL stocks, not just those in predefined list
                            liquid_stocks = [s.strip() for s in str(liquid_stocks_str).split(',') if s.strip() and s.strip() != 'nan']
                            illiquid_stocks = [s.strip() for s in str(illiquid_stocks_str).split(',') if s.strip() and s.strip() != 'nan']
                            
                            all_selected = liquid_stocks + illiquid_stocks
                            
                            if all_selected:
                                hydro_weight_per_stock = 100.0 / len(all_selected)
                                for stock in all_selected:
                                    weight_records.append({
                                        'symbol': stock,
                                        'period': period,
                                        'strategy': 'Alpha',
                                        'weight': hydro_weight_per_stock
                                    })
                        else:
                            # Fallback to equal weight
                            for stock in hydro_stocks:
                                weight_records.append({
                                    'symbol': stock,
                                    'period': period,
                                    'strategy': 'Alpha',
                                    'weight': 100.0 / len(hydro_stocks)
                                })
                
                else:  # Neutral
                    # 50% Hydro + 25% Gas + 25% Coal (specialized portfolios)
                    
                    # Hydro: Before 2Q2020 equal weighted, 2Q2020 equal weighted (baseline), from 3Q2020 flood level
                    if date < pd.to_datetime('2020-04-01') or period == '2020Q2':
                        # Before 2Q2020 or exactly 2Q2020 baseline: use equal weighted hydro
                        for stock in hydro_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 50.0 / len(hydro_stocks)
                            })
                    else:
                        # From 3Q2020 onwards: use flood level portfolio
                        hydro_period_data = hydro_df[hydro_df['period'] == period]
                        if not hydro_period_data.empty:
                            hydro_row = hydro_period_data[hydro_period_data['strategy_type'] == 'flood_level'].iloc[0]
                            liquid_stocks_str = hydro_row['liquid_stock'] if 'liquid_stock' in hydro_row else ''
                            illiquid_stocks_str = hydro_row['illiquid_stock'] if 'illiquid_stock' in hydro_row else ''
                            
                            # Parse stocks - include ALL stocks, not just those in predefined list
                            liquid_stocks = [s.strip() for s in str(liquid_stocks_str).split(',') if s.strip() and s.strip() != 'nan']
                            illiquid_stocks = [s.strip() for s in str(illiquid_stocks_str).split(',') if s.strip() and s.strip() != 'nan']
                            
                            all_selected = liquid_stocks + illiquid_stocks
                            
                            if all_selected:
                                hydro_weight_per_stock = 50.0 / len(all_selected)
                                for stock in all_selected:
                                    weight_records.append({
                                        'symbol': stock,
                                        'period': period,
                                        'strategy': 'Alpha',
                                        'weight': hydro_weight_per_stock
                                    })
                        else:
                            for stock in hydro_stocks:
                                weight_records.append({
                                    'symbol': stock,
                                    'period': period,
                                    'strategy': 'Alpha',
                                    'weight': 50.0 / len(hydro_stocks)
                                })
                    
                    # Gas: specialized portfolio
                    gas_period_data = gas_df[gas_df['quarter'] == period]
                    if not gas_period_data.empty:
                        gas_row = gas_period_data[gas_period_data['strategy_type'] == 'concentrated'].iloc[0]
                        pow_weight = gas_row['pow_weight'] if 'pow_weight' in gas_row else 0.5
                        nt2_weight = gas_row['nt2_weight'] if 'nt2_weight' in gas_row else 0.5
                        
                        # Only add stocks with non-zero weights
                        # Note: weights in CSV are in decimal (1.0 = 100%), convert to percentage and multiply by sector allocation
                        if pow_weight > 0:
                            weight_records.append({
                                'symbol': 'POW',
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': pow_weight * 100 * 0.25  # Convert to % and multiply by 25% gas allocation
                            })
                        
                        if nt2_weight > 0:
                            weight_records.append({
                                'symbol': 'NT2',
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': nt2_weight * 100 * 0.25  # Convert to % and multiply by 25% gas allocation
                            })
                    else:
                        for stock in gas_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 25.0 / len(gas_stocks)
                            })
                    
                    # Coal: specialized portfolio
                    coal_period_data = coal_df[coal_df['period'] == period]
                    if not coal_period_data.empty:
                        coal_row = coal_period_data[coal_period_data['strategy_type'] == 'concentrated'].iloc[0]
                        selected_stocks_str = coal_row['selected_stocks'] if 'selected_stocks' in coal_row else ''
                        
                        selected_coal = [s.strip() for s in str(selected_stocks_str).split(',') if s.strip() in coal_stocks]
                        
                        if selected_coal:
                            coal_weight_per_stock = 25.0 / len(selected_coal)
                            for stock in selected_coal:
                                weight_records.append({
                                    'symbol': stock,
                                    'period': period,
                                    'strategy': 'Alpha',
                                    'weight': coal_weight_per_stock
                                })
                    else:
                        for stock in coal_stocks:
                            weight_records.append({
                                'symbol': stock,
                                'period': period,
                                'strategy': 'Alpha',
                                'weight': 25.0 / len(coal_stocks)
                            })
        
        # Create DataFrame from records
        weights_df = pd.DataFrame(weight_records)
        
        # Sort by period, strategy, symbol
        weights_df = weights_df.sort_values(['period', 'strategy', 'symbol'])
        
        # Round weights to 2 decimal places
        weights_df['weight'] = weights_df['weight'].round(2)
        
        # Export to CSV
        csv_file_path = os.path.join(script_dir, 'data','strategies_results', 'stock_weights.csv')
        weights_df.to_csv(csv_file_path, index=False)
        
        print(f"✅ Successfully exported stock weights to: {csv_file_path}")
        print(f"   Total records: {len(weights_df)}")
        print(f"   Periods: {weights_df['period'].nunique()}")
        print(f"   Strategies: {weights_df['strategy'].nunique()}")
        print(f"   Symbols: {weights_df['symbol'].nunique()}")
        
        try:
            st.success(f"✅ Stock weights exported to: {csv_file_path}")
            st.info(f"Total records: {len(weights_df)} | Periods: {weights_df['period'].nunique()} | Strategies: {weights_df['strategy'].nunique()}")
        except:
            pass
        
        return True
        
    except Exception as e:
        error_msg = f"Error exporting stock weights to CSV: {e}"
        print(error_msg)
        import traceback
        print(traceback.format_exc())
        try:
            st.error(error_msg)
        except:
            pass
        return False


def generate_and_export_strategies_csv():
    """Simple function to generate strategy data and export to CSV - can be called from anywhere"""
    try:
        print("🚀 Generating and exporting trading strategies to CSV...")
        
        # Load ENSO data
        script_dir = os.path.dirname(os.path.abspath(__file__))
        enso_file = os.path.join(script_dir, 'data', 'enso_data_quarterly.csv')
        
        if os.path.exists(enso_file):
            enso_df = pd.read_csv(enso_file)
            print(f"✅ Loaded ENSO data: {len(enso_df)} quarters")
        else:
            print("⚠️ ENSO data file not found, using None")
            enso_df = None
        
        # Generate strategies
        unified_df = create_comprehensive_strategy_comparison(enso_df)
        
        if unified_df is not None and not unified_df.empty:
            print(f"✅ Generated {len(unified_df)} quarters of strategy data")
            # CSV export happens automatically in create_comprehensive_strategy_comparison
            
            # Also export stock weights
            print("\n🔄 Exporting stock weights...")
            if export_stock_weights_to_csv(enso_df):
                print("✅ Stock weights exported successfully")
            else:
                print("⚠️ Failed to export stock weights")
            
            return True
        else:
            print("❌ Failed to generate strategy data")
            return False
            
    except Exception as e:
        print(f"❌ Error in generate_and_export_strategies_csv: {e}")
        return False


def create_unified_strategy_chart(unified_df):
    """Create unified strategy performance chart with robust data type handling"""
    try:
        if unified_df is None or unified_df.empty:
            st.warning("No data available for chart creation")
            return None
        
        # Debug: Check what columns we actually have
        st.info(f"Available columns: {list(unified_df.columns)}")
        
        # Check if required columns exist
        required_cols = ['Period', 'Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']
        missing_cols = [col for col in required_cols if col not in unified_df.columns]
        if missing_cols:
            error_msg = f"Missing columns in data: {missing_cols}"
            st.error(error_msg)
            return None
        
        # Create a clean copy of the data to avoid modifying original
        chart_df = unified_df.copy()
        
        # Robust data cleaning function
        def clean_and_convert_column(col_name):
            """Clean and convert column to numeric, handling all edge cases"""
            try:
                col_data = chart_df[col_name]
                
                # If it's already numeric, just fill NaN with 0
                if pd.api.types.is_numeric_dtype(col_data):
                    return pd.to_numeric(col_data, errors='coerce').fillna(0.0)
                
                # If it's object/string, try various conversions
                if col_data.dtype == 'object':
                    # First, convert to string and remove any non-numeric characters except decimal points and minus signs
                    cleaned = col_data.astype(str).str.replace(r'[^\d.-]', '', regex=True)
                    # Convert to numeric
                    numeric_data = pd.to_numeric(cleaned, errors='coerce').fillna(0.0)
                    return numeric_data
                
                # Fallback: force conversion
                return pd.to_numeric(col_data.astype(str), errors='coerce').fillna(0.0)
                
            except Exception as e:
                st.warning(f"Error cleaning column {col_name}: {e}")
                # Return series of zeros as last resort
                return pd.Series([0.0] * len(chart_df), index=chart_df.index)
        
        # Clean Period column for x-axis
        try:
            if 'Period' in chart_df.columns:
                period_data = chart_df['Period']
                if period_data.dtype == 'object':
                    # Try to convert to datetime
                    try:
                        period_data = pd.to_datetime(period_data)
                    except:
                        # If that fails, try to clean first
                        period_data = pd.to_datetime(period_data.astype(str), errors='coerce')
                        # Fill any NaT with a default date
                        period_data = period_data.fillna(pd.to_datetime('2011-01-01'))
            else:
                # If no Period column, use the index
                period_data = chart_df.index
                if not isinstance(period_data, pd.DatetimeIndex):
                    period_data = pd.to_datetime(period_data, errors='coerce')
        except Exception as e:
            st.warning(f"Error processing Period data: {e}")
            # Create a default date range
            period_data = pd.date_range('2011-01-01', '2025-09-30', periods=len(chart_df))
        
        # Clean all cumulative return columns
        alpha_cum = clean_and_convert_column('Alpha_Cumulative')
        oni_cum = clean_and_convert_column('ONI_Cumulative')
        equal_cum = clean_and_convert_column('Equal_Cumulative')
        vni_cum = clean_and_convert_column('VNI_Cumulative')
        
        # Debug: Show data types and sample values
        st.info(f"Data types - Period: {type(period_data.iloc[0] if len(period_data) > 0 else 'empty')}, "
                f"Alpha: {type(alpha_cum.iloc[0] if len(alpha_cum) > 0 else 'empty')}")
        
        # Create the figure
        fig = go.Figure()
        
        # Add traces for each strategy using cleaned data
        try:
            fig.add_trace(go.Scatter(
                x=period_data,
                y=alpha_cum,
                mode='lines+markers',
                name='Alpha Strategy',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=4),
                hovertemplate='Date: %{x}<br>Alpha Strategy: %{y:.2f}%<extra></extra>'
            ))
        except Exception as e:
            st.error(f"Error adding Alpha trace: {e}")
        
        try:
            fig.add_trace(go.Scatter(
                x=period_data,
                y=oni_cum,
                mode='lines+markers',
                name='ONI Strategy',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=4),
                hovertemplate='Date: %{x}<br>ONI Strategy: %{y:.2f}%<extra></extra>'
            ))
        except Exception as e:
            st.error(f"Error adding ONI trace: {e}")
        
        try:
            fig.add_trace(go.Scatter(
                x=period_data,
                y=equal_cum,
                mode='lines+markers',
                name='Equal Weight',
                line=dict(color='#2ca02c', width=3),
                marker=dict(size=4),
                hovertemplate='Date: %{x}<br>Equal Weight: %{y:.2f}%<extra></extra>'
            ))
        except Exception as e:
            st.error(f"Error adding Equal Weight trace: {e}")
        
        try:
            fig.add_trace(go.Scatter(
                x=period_data,
                y=vni_cum,
                mode='lines+markers',
                name='VNI Benchmark',
                line=dict(color='#d62728', width=3),
                marker=dict(size=4),
                hovertemplate='Date: %{x}<br>VNI Benchmark: %{y:.2f}%<extra></extra>'
            ))
        except Exception as e:
            st.error(f"Error adding VNI trace: {e}")
        
        # Add timeline markers for Alpha strategy (with error handling)
        try:
            fig.add_vline(
                x=pd.to_datetime("2019-01-01"),
                line_dash="dash",
                line_color="gray",
                annotation_text="Gas/Coal Begin"
            )
            
            fig.add_vline(
                x=pd.to_datetime("2020-04-01"),
                line_dash="dash", 
                line_color="gray",
                annotation_text="Full Specialization"
            )
        except Exception as e:
            st.warning(f"Could not add timeline markers: {e}")
        
        # Update layout
        fig.update_layout(
            title='Power Sector Trading Strategies - Cumulative Performance',
            xaxis_title='Date',
            yaxis_title='Cumulative Return (%)',
            hovermode='x unified',
            template='plotly_white',
            height=600,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        st.success("✅ Chart created successfully")
        return fig
        
    except Exception as e:
        error_msg = f"Error creating unified strategy chart: {e}"
        st.error(error_msg)
        print(error_msg)
        import traceback
        st.error(f"Full traceback: {traceback.format_exc()}")
        return None


def convert_df_to_csv(df):
    """Convert DataFrame to CSV for download"""
    return df.to_csv(index=False).encode('utf-8')


def convert_df_to_excel(df, sheet_name="Trading_Strategies"):
    """Convert DataFrame to Excel for download"""
    import io
    if df is None or df.empty:
        # Create a minimal dummy dataframe to avoid Excel errors
        df = pd.DataFrame({"No Data": ["No data available"]})
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()


def plot_cumulative_returns_from_csv():
    """Load and plot cumulative returns from trading_strategies_comparison.csv"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data','strategies_results', 'trading_strategies_comparison.csv')
        
        if not os.path.exists(csv_file):
            st.error(f"❌ File not found: {csv_file}")
            st.error(f"Script directory: {script_dir}")
            return None, None
            
        # Load the CSV data
        df = pd.read_csv(csv_file)
        
        # Check if required columns exist
        required_columns = ['Period', 'Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"❌ Missing required columns: {missing_columns}")
            st.error(f"Available columns: {list(df.columns)}")
            return None, None
        
        # Convert Period to datetime
        df['Period'] = pd.to_datetime(df['Period'])
        
        # Create the cumulative returns plot
        fig = go.Figure()
        
        # Add traces for each strategy
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['Alpha_Cumulative'], 
            name='Alpha Strategy',
            line=dict(color='#1f77b4', width=2),
            hovertemplate='<b>Alpha Strategy</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['ONI_Cumulative'], 
            name='ONI Strategy',
            line=dict(color='#ff7f0e', width=2),
            hovertemplate='<b>ONI Strategy</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['Equal_Cumulative'], 
            name='Equal Weight',
            line=dict(color='#2ca02c', width=2),
            hovertemplate='<b>Equal Weight</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['VNI_Cumulative'], 
            name='VNI Benchmark',
            line=dict(color='#d62728', width=2),
            hovertemplate='<b>VNI Benchmark</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': 'Trading Strategies Cumulative Returns Comparison',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='Period',
            yaxis_title='Cumulative Return (%)',
            height=600,
            hovermode='x unified',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        # Add grid
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        
        return fig, df
        
    except Exception as e:
        st.error(f"❌ Error loading cumulative returns data: {str(e)}")
        import traceback
        st.error(f"Full traceback: {traceback.format_exc()}")
        return None, None


def create_filtered_cumulative_plot(df):
    """Create cumulative returns plot from filtered DataFrame"""
    try:
        # Create the cumulative returns plot
        fig = go.Figure()
        
        # Add traces for each strategy
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['Alpha_Cumulative'], 
            name='Alpha Strategy',
            line=dict(color='#1f77b4', width=2),
            hovertemplate='<b>Alpha Strategy</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['ONI_Cumulative'], 
            name='ONI Strategy',
            line=dict(color='#ff7f0e', width=2),
            hovertemplate='<b>ONI Strategy</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['Equal_Cumulative'], 
            name='Equal Weight',
            line=dict(color='#2ca02c', width=2),
            hovertemplate='<b>Equal Weight</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=df['Period'], 
            y=df['VNI_Cumulative'], 
            name='VNI Benchmark',
            line=dict(color='#d62728', width=2),
            hovertemplate='<b>VNI Benchmark</b><br>Date: %{x}<br>Return: %{y:.2f}%<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': 'Trading Strategies Cumulative Returns Comparison',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='Period',
            yaxis_title='Cumulative Return (%)',
            height=600,
            hovermode='x unified',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        # Add grid
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        
        return fig
        
    except Exception as e:
        st.error(f"❌ Error creating filtered plot: {str(e)}")
        return None


def create_weight_allocation_tables():
    """Create accurate weight allocation tables for Alpha and ONI strategies based on actual implementation"""
    
    # Alpha Strategy Timeline Evolution (Based on actual implementation)
    alpha_timeline = pd.DataFrame({
        'Period': ['Before 1Q2019', '1Q2019 - 1Q2020', '2Q2020 onwards'],
        'Strategy Rule': [
            'Alpha = ONI Strategy (Identical allocation)',
            'ONI-based with Mixed Portfolios (Transition)',
            'ONI-based with Specialized Portfolios'
        ],
        'El Niño (ONI > 0.5)': [
            'Same as ONI: 50% Gas + 50% Coal (Equal Weight)',
            '50% Gas (Specialized Volume) + 50% Coal (Specialized Volume)',
            '50% Gas (Best Contracted Volume) + 50% Coal (Best Sales Volume)'
        ],
        'Neutral (-0.5 ≤ ONI ≤ 0.5)': [
            'Same as ONI: 50% Hydro + 25% Gas + 25% Coal (Sector Weight)',
            '50% Sector Weight + 25% Gas (Specialized) + 25% Coal (Specialized)',
            '50% Hydro (Flood Portfolio) + 25% Gas (Best Contracted) + 25% Coal (Best Sales)'
        ],
        'La Niña (ONI < -0.5)': [
            'Same as ONI: 100% Hydro (Sector Weight)',
            '100% Sector Weight Portfolio (50% Hydro, 25% Gas, 25% Coal)',
            '100% Hydro (Flood Level Portfolio)'
        ]
    })
    
    # Note: Detailed stock weights are now handled by the sector-based table function
    
    # ONI Strategy Sector Weights (Based on ENSO conditions only)
    oni_weights = pd.DataFrame({
        'ENSO Condition': ['El Niño (ONI > 0.5)', 'Neutral (-0.5 ≤ ONI ≤ 0.5)', 'La Niña (ONI < -0.5)'],
        'Hydro Allocation (%)': [0, 50, 100],
        'Gas Allocation (%)': [50, 25, 0],
        'Coal Allocation (%)': [50, 25, 0],
        'Portfolio Type': ['Equal-weighted Gas/Coal', 'Sector-weighted (50% Hydro, 25% Gas, 25% Coal)', 'Equal-weighted Hydro'],
        'Strategy Logic': [
            'Drought: No hydro, 50/50 thermal split',
            'Normal: Balanced allocation across all sectors',
            'High rainfall: 100% hydro, no thermal'
        ]
    })
    
    return alpha_timeline, None, oni_weights


def create_alpha_sector_weight_tables():
    """Create Alpha strategy portfolio weight tables organized by sector (Hydro, Gas, Coal)"""
    
    # Load ENSO data to get actual quarterly ENSO conditions
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        enso_file = os.path.join(script_dir, 'data', 'enso_data_quarterly.csv')
        
        if os.path.exists(enso_file):
            enso_df = pd.read_csv(enso_file)
            
            # Convert quarter format to standard format
            if 'date' in enso_df.columns:
                def convert_quarter_to_label(quarter_str):
                    try:
                        if 'Q' in str(quarter_str):
                            parts = str(quarter_str).split('Q')
                            quarter = parts[0]
                            year_str = parts[1]
                            year = int(f"20{year_str}") if len(year_str) == 2 else int(year_str)
                            return f"{quarter}Q{year}"
                        return str(quarter_str)
                    except:
                        return str(quarter_str)
                
                enso_df['Quarter'] = enso_df['date'].apply(convert_quarter_to_label)
            
            # Define stock lists by sector
            hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
            gas_stocks = ['POW', 'NT2']
            coal_stocks = ['QTP', 'PPC', 'HND']
            
            # Create Alpha strategy sector-based weights
            alpha_sector_weights = []
            
            for index, row in enso_df.iterrows():
                quarter = row['Quarter'] if 'Quarter' in row else f"Q{index+1}"
                oni_value = row.get('ONI', 0)
                
                # Determine ENSO condition
                if oni_value > 0.5:
                    condition = "El Niño"
                elif oni_value < -0.5:
                    condition = "La Niña"
                else:
                    condition = "Neutral"
                
                # Determine timeline period
                try:
                    year = int(quarter.split('Q')[1])
                    q_num = int(quarter.split('Q')[0])
                    
                    if year < 2019:
                        period = "Before 1Q2019"
                    elif year == 2019 or (year == 2020 and q_num == 1):
                        period = "1Q2019-1Q2020"
                    else:
                        period = "2Q2020+"
                except:
                    period = "2Q2020+"
                
                # Determine portfolio allocation based on period, condition, and strategy rules
                hydro_allocation = ""
                gas_allocation = ""
                coal_allocation = ""
                
                if period == "Before 1Q2019":
                    # Alpha = ONI
                    if condition == "El Niño":
                        hydro_allocation = "Not Selected"
                        gas_allocation = "POW: 50%, NT2: 50%"
                        coal_allocation = "QTP: 33.33%, PPC: 33.33%, HND: 33.33%"
                    elif condition == "La Niña":
                        hydro_allocation = "Equal Weight - All 16 hydro stocks (100% total)"
                        gas_allocation = "Not Selected"
                        coal_allocation = "Not Selected"
                    else:  # Neutral
                        hydro_allocation = "Equal Weight - All 16 hydro stocks (50% total)"
                        gas_allocation = "POW: 12.5%, NT2: 12.5% (25% total)"
                        coal_allocation = "QTP: 8.33%, PPC: 8.33%, HND: 8.33% (25% total)"
                
                elif period == "1Q2019-1Q2020":
                    if condition == "El Niño":
                        hydro_allocation = "Not Selected"
                        gas_allocation = "Specialized Portfolio (50% total)"
                        coal_allocation = "Specialized Portfolio (50% total)"
                    elif condition == "La Niña":
                        hydro_allocation = "Equal Weight - All 16 hydro stocks (100% total)"
                        gas_allocation = "Not Selected"
                        coal_allocation = "Not Selected"
                    else:  # Neutral
                        hydro_allocation = "Equal Weight - All 16 hydro stocks (50% total)"
                        gas_allocation = "Specialized Portfolio (25% total)"
                        coal_allocation = "Specialized Portfolio (25% total)"
                
                else:  # 2Q2020+
                    # Special handling for 2Q2020 baseline
                    if quarter == "2Q2020" and condition == "Neutral":
                        hydro_allocation = "Equal Weight - All 16 hydro stocks (50% total)"
                        gas_allocation = "Best Contracted Volume Portfolio (25% total)"
                        coal_allocation = "Best Sales Volume Portfolio (25% total)"
                    elif condition == "El Niño":
                        hydro_allocation = "Not Selected"
                        gas_allocation = "Best Contracted Volume Portfolio (50% total)"
                        coal_allocation = "Best Sales Volume Portfolio (50% total)"
                    elif condition == "La Niña":
                        hydro_allocation = "Flood Level Portfolio (100% total)"
                        gas_allocation = "Not Selected"
                        coal_allocation = "Not Selected"
                    else:  # Neutral (from 3Q2020+)
                        hydro_allocation = "Flood Level Portfolio (50% total)"
                        gas_allocation = "Best Contracted Volume Portfolio (25% total)"
                        coal_allocation = "Best Sales Volume Portfolio (25% total)"
                
                # Create quarterly record
                quarterly_record = {
                    'Quarter': quarter,
                    'ONI_Value': f"{oni_value:.3f}",
                    'ENSO_Condition': condition,
                    'Period': period,
                    'Hydro_Portfolio': hydro_allocation,
                    'Gas_Portfolio': gas_allocation,
                    'Coal_Portfolio': coal_allocation
                }
                alpha_sector_weights.append(quarterly_record)
            
            alpha_sector_df = pd.DataFrame(alpha_sector_weights)
            return alpha_sector_df
            
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error creating Alpha sector weight tables: {e}")
        return pd.DataFrame()


def create_quarterly_weight_tables():
    """Create detailed quarterly weight allocation tables showing exact weights for each quarter"""
    
    # Load ENSO data to get actual quarterly ENSO conditions
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        enso_file = os.path.join(script_dir, 'data', 'enso_data_quarterly.csv')
        
        if os.path.exists(enso_file):
            enso_df = pd.read_csv(enso_file)
            
            # Convert quarter format to standard format
            if 'date' in enso_df.columns:
                def convert_quarter_to_label(quarter_str):
                    try:
                        if 'Q' in str(quarter_str):
                            parts = str(quarter_str).split('Q')
                            quarter = parts[0]
                            year_str = parts[1]
                            year = int(f"20{year_str}") if len(year_str) == 2 else int(year_str)
                            return f"{quarter}Q{year}"
                        return str(quarter_str)
                    except:
                        return str(quarter_str)
                
                enso_df['Quarter'] = enso_df['date'].apply(convert_quarter_to_label)
            
            # Create Alpha strategy quarterly weights
            alpha_quarterly_weights = []
            oni_quarterly_weights = []

            # Define stock-to-sector mapping (corrected)
            stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP', 'POW', 'NT2', 'QTP', 'PPC', 'HND']
            sectors = ['Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Hydro', 'Gas', 'Gas', 'Coal', 'Coal', 'Coal']
            
            for index, row in enso_df.iterrows():
                quarter = row['Quarter'] if 'Quarter' in row else f"Q{index+1}"
                oni_value = row.get('ONI', 0)
                
                # Determine ENSO condition
                if oni_value > 0.5:
                    condition = "El Niño"
                elif oni_value < -0.5:
                    condition = "La Niña"
                else:
                    condition = "Neutral"
                
                # Determine timeline period
                try:
                    year = int(quarter.split('Q')[1])
                    q_num = int(quarter.split('Q')[0])
                    
                    if year < 2019:
                        period = "Before 1Q2019"
                    elif year == 2019 or (year == 2020 and q_num == 1):
                        period = "1Q2019-1Q2020"
                    else:
                        period = "2Q2020+"
                except:
                    period = "2Q2020+"
                
                # Alpha Strategy Weights
                alpha_weights = {}
                oni_sector_weights = {}
                
                # ONI Strategy (sector allocation)
                if condition == "El Niño":
                    oni_sector_weights = {"Hydro": 0, "Gas": 50, "Coal": 50}
                elif condition == "La Niña":
                    oni_sector_weights = {"Hydro": 100, "Gas": 0, "Coal": 0}
                else:  # Neutral
                    oni_sector_weights = {"Hydro": 50, "Gas": 25, "Coal": 25}
                
                # Alpha Strategy (stock-level allocation)
                if period == "Before 1Q2019":
                    # Alpha = ONI
                    if condition == "El Niño":
                        # Initialize all stocks to 0
                        alpha_weights = {stock: 0 for stock in stocks}
                        # Gas stocks get 25% each, Coal stocks get 16.67% each
                        for i, stock in enumerate(stocks):
                            if sectors[i] == "Gas":
                                alpha_weights[stock] = 25  # POW: 25%, NT2: 25%
                            elif sectors[i] == "Coal":
                                alpha_weights[stock] = 16.67  # QTP: 16.67%, PPC: 16.67%, HND: 16.67%
                    elif condition == "La Niña":
                        # 100% Hydro - equally weighted among 16 stocks
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 6.25  # 100% / 16 stocks = 6.25% each
                    else:  # Neutral
                        # Equal weight: Hydro (50% total), Gas (25% total), Coal (25% total)
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        gas_stocks = ['POW', 'NT2']
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 3.125  # 50% / 16 stocks = 3.125% each
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 12.5  # 25% / 2 stocks = 12.5% each  
                        for stock in coal_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 8.33  # 25% / 3 stocks = 8.33% each
                
                elif period == "1Q2019-1Q2020":
                    if condition == "El Niño":
                        # Gas + Coal specialized portfolios (50% each sector)
                        alpha_weights = {stock: 0 for stock in stocks}
                        gas_stocks = ['POW', 'NT2']
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Gas Portfolio (25% each)'
                        for stock in coal_stocks:  
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Coal Portfolio (16.67% each)'
                    elif condition == "La Niña":
                        # Equal weight all stocks (transition period)
                        num_stocks = len([s for s in stocks if s in ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP', 'POW', 'NT2', 'QTP', 'PPC', 'HND']])
                        weight_per_stock = 100 / num_stocks if num_stocks > 0 else 0
                        alpha_weights = {stock: f"{weight_per_stock:.2f}%" if stock in ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP', 'POW', 'NT2', 'QTP', 'PPC', 'HND'] else 0 for stock in stocks}
                    else:  # Neutral
                        # 50% Hydro (equal weighted) + 25% Gas (specialized) + 25% Coal (specialized)
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        gas_stocks = ['POW', 'NT2']  
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        # Hydro: 50% equally weighted among 16 stocks = 3.125% each
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 3.125
                        
                        # Gas: 25% from specialized portfolio
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Gas Portfolio (25% total)'
                        
                        # Coal: 25% from specialized portfolio
                        for stock in coal_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Coal Portfolio (25% total)'
                
                else:  # 2Q2020+
                    # Special handling for 2Q2020 baseline period
                    if quarter == "2Q2020" and condition == "Neutral":
                        # 2Q2020 baseline: use equal weighted hydro (50%) + specialized gas/coal (25% each)
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        gas_stocks = ['POW', 'NT2']
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 3.125  # 50% / 16 stocks = 3.125% each
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Contracted Volume Portfolio (25% total)'
                        for stock in coal_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Sales Volume Portfolio (25% total)'
                    elif condition == "El Niño":
                        # Gas + Coal specialized portfolios (50% each sector)
                        alpha_weights = {stock: 0 for stock in stocks}
                        gas_stocks = ['POW', 'NT2']
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Contracted Volume Portfolio (50% total)'
                        for stock in coal_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Sales Volume Portfolio (50% total)'
                    elif condition == "La Niña":
                        # Hydro flood portfolio (100% hydro sector)
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Flood Level Portfolio (100% total)'
                    else:  # Neutral (from 3Q2020+)
                        # 50% Hydro Flood + 25% Gas specialized + 25% Coal specialized
                        alpha_weights = {stock: 0 for stock in stocks}
                        hydro_stocks = ['REE', 'PC1', 'HDG', 'GEG', 'TTA', 'DPG', 'AVC', 'GHC', 'VPD', 'DRL', 'S4A', 'SBA', 'VSH', 'NED', 'TMP', 'HNA', 'SHP']
                        gas_stocks = ['POW', 'NT2']
                        coal_stocks = ['QTP', 'PPC', 'HND']
                        
                        for stock in hydro_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Flood Level Portfolio (50% total)'
                        for stock in gas_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Contracted Volume Portfolio (25% total)'
                        for stock in coal_stocks:
                            if stock in alpha_weights:
                                alpha_weights[stock] = 'Best Sales Volume Portfolio (25%)'
                
                # Create quarterly record
                quarterly_record = {
                    'Quarter': quarter,
                    'ONI_Value': f"{oni_value:.3f}",
                    'ENSO_Condition': condition,
                    'Period': period
                }
                # Handle mixed data types in alpha_weights (strings and numbers)
                for stock in stocks:
                    weight_value = alpha_weights.get(stock, 0)
                    if isinstance(weight_value, str):
                        quarterly_record[stock] = weight_value
                    else:
                        quarterly_record[stock] = f"{weight_value:.2f}%" if weight_value != 0 else "0.00%"
                alpha_quarterly_weights.append(quarterly_record)
                
                # ONI quarterly record  
                oni_record = {
                    'Quarter': quarter,
                    'ONI_Value': f"{oni_value:.3f}",
                    'ENSO_Condition': condition,
                    'Hydro_Sector': f"{oni_sector_weights['Hydro']:.1f}%",
                    'Gas_Sector': f"{oni_sector_weights['Gas']:.1f}%",
                    'Coal_Sector': f"{oni_sector_weights['Coal']:.1f}%"
                }
                oni_quarterly_weights.append(oni_record)
            
            alpha_quarterly_df = pd.DataFrame(alpha_quarterly_weights)
            oni_quarterly_df = pd.DataFrame(oni_quarterly_weights)
            
            return alpha_quarterly_df, oni_quarterly_df
            
        else:
            return pd.DataFrame(), pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error creating quarterly weight tables: {e}")
        return pd.DataFrame(), pd.DataFrame()


def display_simple_cumulative_returns():
    """Simple function to display only cumulative returns plot from CSV - for app_new.py"""
    st.title("📈 Trading Strategy Analysis")
    
    # Introduction
    st.markdown("""
    ### Power Sector Trading Strategies Comparison
    
    Compare cumulative returns across four distinct investment strategies:
    
    **🎯 Alpha Strategy**: 
    - **Before 1Q2019**: Identical to ONI Strategy (ENSO-based allocation)
    - **From 1Q2019**: ENSO-based allocation using specialized portfolios:
      - El Niño: 50% Gas (Best Contracted Volume Growth) + 50% Coal (Best Sales Volume Growth)
      - Neutral: 50% Hydro (Flood Portfolio) + 25% Gas (Best Contracted) + 25% Coal (Best Sales)  
      - La Niña: 100% Hydro (Flood Level Portfolio based on reservoir levels)
    
    **🌊 ONI Strategy**: Pure ENSO-based seasonal allocation using equal-weighted sector portfolios:
    - El Niño (drought): 50% Gas + 50% Coal (no Hydro)
    - Neutral: 50% Hydro + 25% Gas + 25% Coal
    - La Niña (high rainfall): 100% Hydro (no thermal)
    
    **⚖️ Equal Weight**: Sector-weighted portfolio (50% Hydro / 25% Gas / 25% Coal, equal-weighted within sectors)
    
    **📊 VNI Benchmark**: Vietnam stock market reference index
    """)
    
    # Time Period Controls
    st.subheader("⏰ Time Period Selection")
    
    # Load data first to get available periods
    try:
        _, full_csv_df = plot_cumulative_returns_from_csv()
        if full_csv_df is not None:
            # Convert Period to datetime and extract quarters
            full_csv_df['Period'] = pd.to_datetime(full_csv_df['Period'])
            full_csv_df['Quarter_Label'] = full_csv_df['Period'].dt.to_period('Q').astype(str)
            available_quarters = full_csv_df['Quarter_Label'].tolist()
            
            # Time period selection controls
            period_col1, period_col2 = st.columns(2)
            
            with period_col1:
                start_quarter = st.selectbox(
                    "📅 Start Quarter:",
                    options=available_quarters,
                    index=0,
                    key="start_quarter"
                )
            
            with period_col2:
                end_quarter = st.selectbox(
                    "📅 End Quarter:",
                    options=available_quarters,
                    index=len(available_quarters)-1,
                    key="end_quarter"
                )
            
            # Filter data based on selected period
            start_idx = available_quarters.index(start_quarter)
            end_idx = available_quarters.index(end_quarter)
            
            if start_idx > end_idx:
                st.error("❌ Start quarter must be before or equal to end quarter!")
                return
                
            filtered_csv_df = full_csv_df.iloc[start_idx:end_idx+1].copy()
            
            # Recalculate cumulative returns to start from 0% at the selected start quarter
            if not filtered_csv_df.empty:
                strategies = ['Alpha', 'ONI', 'Equal', 'VNI']
                for strategy in strategies:
                    return_col = f'{strategy}_Return'
                    cumulative_col = f'{strategy}_Cumulative'
                    
                    if return_col in filtered_csv_df.columns:
                        # Reset index to ensure proper calculation
                        filtered_csv_df.reset_index(drop=True, inplace=True)
                        
                        # Start cumulative returns from 0% at the beginning quarter
                        # First quarter shows 0%, subsequent quarters show cumulative returns
                        cumulative_returns = []
                        
                        for i in range(len(filtered_csv_df)):
                            if i == 0:
                                # First quarter (beginning quarter) = 0%
                                cumulative_returns.append(0.0)
                            else:
                                # Calculate cumulative return from the second quarter onwards
                                # Using returns from quarter 2 onwards (index 1+)
                                returns_slice = filtered_csv_df[return_col].iloc[1:i+1]
                                cumulative_factor = (1 + returns_slice/100).prod()
                                cumulative_return = (cumulative_factor - 1) * 100
                                cumulative_returns.append(cumulative_return)
                        
                        filtered_csv_df[cumulative_col] = cumulative_returns
        else:
            st.error("❌ Could not load data for period selection")
            return
    except Exception as e:
        st.error(f"Error loading data for period selection: {str(e)}")
        return
    
    # Plot cumulative returns from filtered CSV
    st.subheader("📈 Cumulative Returns from Historical Data")
    
    # Add informational note about recalculated returns
    if start_quarter != available_quarters[0] or end_quarter != available_quarters[-1]:
        st.info(f"📊 **Note**: Cumulative returns have been recalculated to start from 0% at {start_quarter} for the selected time period.")
    
    # Create filtered plot
    try:
        fig = create_filtered_cumulative_plot(filtered_csv_df)
    except Exception as e:
        st.error(f"Error creating plot: {str(e)}")
        fig = None
    
    if fig and filtered_csv_df is not None:
        st.plotly_chart(fig, use_container_width=True)
        
        # Performance Summary Cards
        st.subheader("📊 Performance Summary")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            final_alpha = filtered_csv_df['Alpha_Cumulative'].iloc[-1]
            st.metric("Alpha Strategy", f"{final_alpha:.1f}%")
        
        with metric_col2:
            final_oni = filtered_csv_df['ONI_Cumulative'].iloc[-1]
            st.metric("ONI Strategy", f"{final_oni:.1f}%")
        
        with metric_col3:
            final_equal = filtered_csv_df['Equal_Cumulative'].iloc[-1]
            st.metric("Equal Weight", f"{final_equal:.1f}%")
        
        with metric_col4:
            final_vni = filtered_csv_df['VNI_Cumulative'].iloc[-1]
            st.metric("VNI Benchmark", f"{final_vni:.1f}%")
        
        # Detailed Performance Tables
        st.subheader("📋 Detailed Returns Analysis")
        
        # Create tabs for different analysis views
        table_tab1, table_tab2 = st.tabs(["📊 Portfolio Returns", "⚖️ Portfolio Weights"])
        
        with table_tab1:
            # Detailed returns table
            st.markdown("#### Quarterly and Cumulative Returns")
            
            # Prepare display dataframe
            display_df = filtered_csv_df[['Quarter_Label', 'Alpha_Return', 'ONI_Return', 'Equal_Return', 'VNI_Return',
                                         'Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']].copy()
            
            # Rename columns for better display
            display_df.columns = ['Quarter', 'Alpha Quarterly (%)', 'ONI Quarterly (%)', 'Equal Quarterly (%)', 'VNI Quarterly (%)',
                                'Alpha Cumulative (%)', 'ONI Cumulative (%)', 'Equal Cumulative (%)', 'VNI Cumulative (%)']
            
            # Format percentages
            percentage_cols = [col for col in display_df.columns if '(%)' in col]
            for col in percentage_cols:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")
            
            # Display the table
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Summary statistics
            st.markdown("#### Summary Statistics")
            
            # Calculate statistics for the filtered period
            stats_data = {
                'Strategy': ['Alpha Strategy', 'ONI Strategy', 'Equal Weight', 'VNI Benchmark'],
                'Total Return (%)': [
                    f"{final_alpha:.2f}%",
                    f"{final_oni:.2f}%", 
                    f"{final_equal:.2f}%",
                    f"{final_vni:.2f}%"
                ],
                'Avg Quarterly Return (%)': [
                    f"{filtered_csv_df['Alpha_Return'].mean():.2f}%",
                    f"{filtered_csv_df['ONI_Return'].mean():.2f}%",
                    f"{filtered_csv_df['Equal_Return'].mean():.2f}%",
                    f"{filtered_csv_df['VNI_Return'].mean():.2f}%"
                ],
                'Volatility (%)': [
                    f"{filtered_csv_df['Alpha_Return'].std():.2f}%",
                    f"{filtered_csv_df['ONI_Return'].std():.2f}%",
                    f"{filtered_csv_df['Equal_Return'].std():.2f}%",
                    f"{filtered_csv_df['VNI_Return'].std():.2f}%"
                ],
                'Best Quarter (%)': [
                    f"{filtered_csv_df['Alpha_Return'].max():.2f}%",
                    f"{filtered_csv_df['ONI_Return'].max():.2f}%",
                    f"{filtered_csv_df['Equal_Return'].max():.2f}%",
                    f"{filtered_csv_df['VNI_Return'].max():.2f}%"
                ],
                'Worst Quarter (%)': [
                    f"{filtered_csv_df['Alpha_Return'].min():.2f}%",
                    f"{filtered_csv_df['ONI_Return'].min():.2f}%",
                    f"{filtered_csv_df['Equal_Return'].min():.2f}%",
                    f"{filtered_csv_df['VNI_Return'].min():.2f}%"
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
        
        with table_tab2:
            # Portfolio weight allocation tables
            st.markdown("#### Portfolio Weight Allocations")
            
            # Create tabs for different weight views
            weight_tab1, weight_tab2 = st.tabs(["🎯 Alpha Detailed Weights", "🌊 ONI Detailed Weights"])
            
            with weight_tab1:
                st.markdown("**🎯 Alpha Strategy - Stocks by Quarter**")
                st.markdown("""
                This table shows the actual stocks selected and their exact weights for each quarter in the Alpha strategy.
                
                **Data Source**: Loaded from `stock_weights.csv` - only shows stocks with non-zero allocations.
                """)
                
                # Load stock weights from CSV
                try:
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    weights_file = os.path.join(script_dir, 'data','strategies_results', 'stock_weights.csv')
                    
                    if os.path.exists(weights_file):
                        weights_df = pd.read_csv(weights_file)
                        
                        # Filter for Alpha strategy only and non-zero weights
                        alpha_weights = weights_df[
                            (weights_df['strategy'] == 'Alpha') & 
                            (weights_df['weight'] > 0)
                        ].copy()
                        
                        if not alpha_weights.empty:
                            # Rename columns for better display
                            alpha_weights = alpha_weights.rename(columns={
                                'symbol': 'Stock Symbol',
                                'period': 'Quarter',
                                'weight': 'Weight (%)'
                            })
                            
                            # Select only the columns we need
                            display_df = alpha_weights[['Quarter', 'Stock Symbol', 'Weight (%)']].copy()
                            
                            # Sort by Quarter and Weight
                            display_df = display_df.sort_values(['Quarter', 'Weight (%)'], ascending=[True, False])
                            
                            # Show stock list by quarter
                            # Group by quarter and show stocks with weights
                            quarter_groups = display_df.groupby('Quarter').apply(
                                lambda x: ', '.join([f"{row['Stock Symbol']} ({row['Weight (%)']:.1f}%)" 
                                                    for _, row in x.iterrows()])
                            ).reset_index()
                            quarter_groups.columns = ['Quarter', 'Stocks & Weights']
                            
                            st.dataframe(
                                quarter_groups,
                                use_container_width=True,
                                hide_index=True,
                                height=600
                            )
                            
                        else:
                            st.warning("⚠️ No Alpha strategy weights found in stock_weights.csv")
                    else:
                        st.error(f"❌ Stock weights file not found: {weights_file}")
                        st.info("💡 Please run `python trading_strategies.py` to generate stock_weights.csv")
                        
                except Exception as e:
                    st.error(f"❌ Error loading Alpha stock weights: {e}")
                    st.error(traceback.format_exc())
            
            with weight_tab2:
                st.markdown("**🌊 ONI Strategy - Exact Quarterly Sector Weights**")
                st.markdown("""
                This table shows the exact sector allocation for each quarter based on ENSO conditions.
                Each sector uses equal-weighted portfolios within the allocation:
                """)
                
                # Get quarterly weight tables
                _, oni_quarterly_df = create_quarterly_weight_tables()
                
                if not oni_quarterly_df.empty:
                    # Add explanatory note about the data
                    st.info("💡 **Sector Allocation**: ONI strategy allocates to sectors based purely on ENSO conditions. Within each sector, stocks are equally weighted.")
                    
                    # Display the ONI quarterly table
                    st.dataframe(
                        oni_quarterly_df, 
                        use_container_width=True, 
                        hide_index=True,
                        height=400
                    )
                    
        # Download Options
        st.subheader("💾 Export Data")
        col1, col2 = st.columns(2)
        
        with col1:
            csv_data = convert_df_to_csv(filtered_csv_df)
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name=f"trading_strategies_comparison_{start_quarter}_to_{end_quarter}.csv",
                mime="text/csv"
            )
        
        with col2:
            excel_data = convert_df_to_excel(filtered_csv_df)
            st.download_button(
                label="📊 Download Excel",
                data=excel_data,
                file_name=f"trading_strategies_comparison_{start_quarter}_to_{end_quarter}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    else:
        st.error("❌ Unable to load trading strategies comparison data from CSV file.")
        # Show debug information
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data','strategies_results', 'trading_strategies_comparison.csv')
        st.error(f"Expected file location: {csv_file}")
        st.error(f"File exists: {os.path.exists(csv_file)}")
        
        # Try to show the first few rows of the CSV for debugging
        try:
            if os.path.exists(csv_file):
                debug_df = pd.read_csv(csv_file)
                st.subheader("🔍 CSV File Debug Info")
                st.write(f"Shape: {debug_df.shape}")
                st.write(f"Columns: {list(debug_df.columns)}")
                st.write("First 5 rows:")
                st.dataframe(debug_df.head())
        except Exception as debug_error:
            st.error(f"Debug error: {str(debug_error)}")


def display_trading_strategies_page():
    """Main function to display the trading strategies page content"""
    st.title("📈 Trading Strategy Analysis")
    
    # Introduction
    st.markdown("""
    ### Power Sector Trading Strategies Comparison
    
    Compare cumulative returns across four distinct investment strategies:
    - **Alpha Strategy**: Timeline-based specialized strategy (Equal → Gas/Coal → Full specialization)
    - **ONI Strategy**: ENSO-based seasonal allocation strategy
    - **Equal Weight**: Sector-weighted portfolio (50% Hydro, 25% Gas, 25% Coal)
    - **VNI Benchmark**: Vietnam stock market reference
    """)
    
    # First, try to plot from existing CSV data
    st.subheader("📈 Cumulative Returns from Historical Data")
    fig, csv_df = plot_cumulative_returns_from_csv()
    
    if fig and csv_df is not None:
        st.plotly_chart(fig, use_container_width=True)
        
        # Performance Summary Cards
        st.subheader("📊 Performance Summary")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            final_alpha = csv_df['Alpha_Cumulative'].iloc[-1]
            st.metric("Alpha Strategy", f"{final_alpha:.1f}%")
        
        with metric_col2:
            final_oni = csv_df['ONI_Cumulative'].iloc[-1]
            st.metric("ONI Strategy", f"{final_oni:.1f}%")
        
        with metric_col3:
            final_equal = csv_df['Equal_Cumulative'].iloc[-1]
            st.metric("Equal Weight", f"{final_equal:.1f}%")
        
        with metric_col4:
            final_vni = csv_df['VNI_Cumulative'].iloc[-1]
            st.metric("VNI Benchmark", f"{final_vni:.1f}%")
        
        # Key Insights
        st.subheader("🔍 Key Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Strategy ranking
            strategies = {
                'Alpha Strategy': final_alpha,
                'ONI Strategy': final_oni,
                'Equal Weight': final_equal,
                'VNI Benchmark': final_vni
            }
            ranked_strategies = sorted(strategies.items(), key=lambda x: x[1], reverse=True)
            
            st.markdown("**📋 Strategy Ranking (by Total Return):**")
            for i, (strategy, return_val) in enumerate(ranked_strategies, 1):
                emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "4️⃣"
                st.markdown(f"{emoji} {strategy}: {return_val:.1f}%")
        
        with col2:
            # Performance periods
            st.markdown("**📅 Implementation Timeline:**")
            st.markdown("""
            - **Before 1Q2019**: Alpha = ONI (Equal weighting)
            - **1Q2019**: Gas/Coal strategies begin
            - **2Q2020**: Full specialization (Hydro strategies)
            - **Current**: All strategies active
            """)
        
        # Detailed Data Table
        with st.expander("📋 View Detailed Strategy Data"):
            display_df = csv_df[['Period', 'Alpha_Return', 'ONI_Return', 'Equal_Return', 'VNI_Return',
                               'Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']].copy()
            
            # Format the data for better display
            for col in ['Alpha_Return', 'ONI_Return', 'Equal_Return', 'VNI_Return']:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")
            for col in ['Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")
            
            st.dataframe(display_df, use_container_width=True)
        
        # Download Options
        st.subheader("💾 Export Data")
        col1, col2 = st.columns(2)
        
        with col1:
            csv_data = convert_df_to_csv(csv_df)
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name="trading_strategies_comparison.csv",
                mime="text/csv"
            )
        
        with col2:
            excel_data = convert_df_to_excel(csv_df)
            st.download_button(
                label="📊 Download Excel",
                data=excel_data,
                file_name="trading_strategies_comparison.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    else:
        st.warning("⚠️ Could not load historical data from CSV. Trying dynamic strategy generation...")
    
    # Strategy Analysis with dynamic generation
    st.subheader("🔄 Dynamic Strategy Analysis")
    with st.spinner("Loading strategy data..."):
        try:
            # Load ENSO data for ONI strategy
            script_dir = os.path.dirname(os.path.abspath(__file__))
            try:
                enso_df = pd.read_csv(os.path.join(script_dir, 'data',  'enso_data_quarterly.csv'))
            except FileNotFoundError:
                st.warning("ENSO data file not found. Using mock data for demonstration.")
                dates = pd.date_range('2011-01-01', '2025-09-30', freq='Q')
                enso_df = pd.DataFrame({
                    'Period': dates,
                    'ONI': np.random.normal(0, 1.2, len(dates))
                })
            
            # Generate unified strategy comparison
            unified_df = create_comprehensive_strategy_comparison(enso_df)
            
            if unified_df is not None and not unified_df.empty:
                # Performance Summary Cards
                st.subheader("📊 Dynamic Performance Summary")
                metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                
                with metric_col1:
                    final_alpha = unified_df['Alpha_Cumulative'].iloc[-1]
                    st.metric("Alpha Strategy", f"{final_alpha:.1f}%")
                
                with metric_col2:
                    final_oni = unified_df['ONI_Cumulative'].iloc[-1]
                    st.metric("ONI Strategy", f"{final_oni:.1f}%")
                
                with metric_col3:
                    final_equal = unified_df['Equal_Cumulative'].iloc[-1]
                    st.metric("Equal Weight", f"{final_equal:.1f}%")
                
                with metric_col4:
                    final_vni = unified_df['VNI_Cumulative'].iloc[-1]
                    st.metric("VNI Benchmark", f"{final_vni:.1f}%")
                
                # Main Cumulative Returns Chart
                st.subheader("📈 Dynamic Cumulative Returns Comparison")
                
                unified_chart = create_unified_strategy_chart(unified_df)
                if unified_chart:
                    st.plotly_chart(unified_chart, use_container_width=True)
                
                # Add export button for manual CSV generation
                st.subheader("💾 Export Generated Data")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🔄 Update CSV File", help="Export current strategy data to trading_strategies_comparison.csv"):
                        if export_strategy_comparison_to_csv(unified_df):
                            st.success("✅ Successfully updated trading_strategies_comparison.csv in data folder!")
                        else:
                            st.error("❌ Failed to export data to CSV file")
                
                with col2:
                    # Download button for immediate download
                    csv_data = convert_df_to_csv(unified_df)
                    st.download_button(
                        label="📄 Download Current Data as CSV",
                        data=csv_data,
                        file_name="trading_strategies_comparison_current.csv",
                        mime="text/csv",
                        help="Download the current strategy comparison data"
                    )
                
            else:
                st.error("❌ Failed to generate strategy comparison data")
                
        except Exception as e:
            st.error(f"❌ Error in strategy analysis: {str(e)}")
            
            # Show fallback demo chart
            st.markdown("### 🚧 Demo Mode")
            st.info("Showing demonstration data. Please check module dependencies.")
            
            dates = pd.date_range('2011-01-01', '2025-09-30', freq='Q')
            demo_data = {
                'Period': dates,
                'Alpha': np.cumsum(np.random.normal(2, 5, len(dates))),
                'ONI': np.cumsum(np.random.normal(1.5, 4, len(dates))),
                'Equal': np.cumsum(np.random.normal(1, 3, len(dates))),
                'VNI': np.cumsum(np.random.normal(1.2, 3.5, len(dates)))
            }
            demo_df = pd.DataFrame(demo_data)
            
            demo_fig = go.Figure()
            demo_fig.add_trace(go.Scatter(x=demo_df['Period'], y=demo_df['Alpha'], name='Alpha Strategy', line=dict(color='#1f77b4')))
            demo_fig.add_trace(go.Scatter(x=demo_df['Period'], y=demo_df['ONI'], name='ONI Strategy', line=dict(color='#ff7f0e')))
            demo_fig.add_trace(go.Scatter(x=demo_df['Period'], y=demo_df['Equal'], name='Equal Weight', line=dict(color='#2ca02c')))
            demo_fig.add_trace(go.Scatter(x=demo_df['Period'], y=demo_df['VNI'], name='VNI Benchmark', line=dict(color='#d62728')))
            
            demo_fig.update_layout(
                title="Demo: Strategy Performance Comparison",
                xaxis_title="Period",
                yaxis_title="Cumulative Return (%)",
                height=400
            )
            
            st.plotly_chart(demo_fig, use_container_width=True)
            st.caption("*This is demonstration data. Actual results will be shown when all modules are available.*")


# Main execution block - automatically export CSV when script is run directly
if __name__ == "__main__":
    print("🚀 Running Trading Strategies Analysis...")
    
    try:
        # Check if we already have a CSV file with data
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data','strategies_results', 'trading_strategies_comparison.csv')
        
        if os.path.exists(csv_file):
            # Load existing CSV and verify it has recent data
            try:
                existing_df = pd.read_csv(csv_file)
                print(f"📊 Found existing CSV with {len(existing_df)} quarters of data")
                
                # Show current performance summary from existing data
                if not existing_df.empty and all(col in existing_df.columns for col in ['Alpha_Cumulative', 'ONI_Cumulative', 'Equal_Cumulative', 'VNI_Cumulative']):
                    print("\n📈 Current Performance Summary (from existing CSV):")
                    print(f"   Alpha Strategy: {existing_df['Alpha_Cumulative'].iloc[-1]:.2f}%")
                    print(f"   ONI Strategy: {existing_df['ONI_Cumulative'].iloc[-1]:.2f}%")
                    print(f"   Equal Weight: {existing_df['Equal_Cumulative'].iloc[-1]:.2f}%")
                    print(f"   VNI Benchmark: {existing_df['VNI_Cumulative'].iloc[-1]:.2f}%")
                
                print(f"✅ CSV file is available at: {csv_file}")
                
                # Also check and generate stock_weights.csv if needed
                stock_weights_file = os.path.join(script_dir, 'data','strategies_results', 'stock_weights.csv')
                if not os.path.exists(stock_weights_file):
                    print("\n📊 Stock weights CSV not found, generating...")
                    enso_file = os.path.join(script_dir, 'data', 'enso_data_quarterly.csv')
                    if os.path.exists(enso_file):
                        enso_df = load_enso_data()  # Use load_enso_data() to get properly formatted data
                        if export_stock_weights_to_csv(enso_df):
                            print("✅ Stock weights CSV generated successfully!")
                    else:
                        print("⚠️ Cannot generate stock weights without ENSO data")
                else:
                    print(f"✅ Stock weights CSV is available at: {stock_weights_file}")
                
                print("\n✅ Trading strategies analysis completed successfully!")
                print("💡 To regenerate with fresh data, run within Streamlit app or delete existing CSVs")
                
            except Exception as read_error:
                print(f"⚠️ Could not read existing CSV: {read_error}")
                print("🔄 Attempting to regenerate...")
                raise Exception("CSV read failed, regenerating")
        else:
            print("📊 No existing CSV found, generating new data...")
            raise Exception("No CSV found, generating new data")
            
    except Exception as e:
        print(f"🔄 Generating fresh strategy comparison data...")
        
        try:
            # Load ENSO data for ONI strategy  
            enso_file = os.path.join(script_dir, 'data', 'enso_data_quarterly.csv')
            
            if os.path.exists(enso_file):
                enso_df = pd.read_csv(enso_file)
                print(f"✅ Loaded ENSO data: {len(enso_df)} quarters")
            else:
                print("⚠️ ENSO data file not found, using None for calculations")
                enso_df = None
            
            # Try to generate comprehensive strategy comparison (may require network/API calls)
            print("📊 Generating comprehensive strategy comparison...")
            print("⚠️ Note: This may take time as it requires fetching stock data...")
            
            # Import required modules at runtime to avoid initial load issues
            import warnings
            warnings.filterwarnings('ignore')  # Suppress Streamlit warnings when running standalone
            
            unified_df = create_comprehensive_strategy_comparison(enso_df)
            
            if unified_df is not None and not unified_df.empty:
                print(f"✅ Generated strategy data: {len(unified_df)} periods")
                
                # The CSV export is automatically triggered within create_comprehensive_strategy_comparison()
                # Show a summary of results
                print("\n📈 Final Performance Summary:")
                print(f"   Alpha Strategy: {unified_df['Alpha_Cumulative'].iloc[-1]:.2f}%")
                print(f"   ONI Strategy: {unified_df['ONI_Cumulative'].iloc[-1]:.2f}%") 
                print(f"   Equal Weight: {unified_df['Equal_Cumulative'].iloc[-1]:.2f}%")
                print(f"   VNI Benchmark: {unified_df['VNI_Cumulative'].iloc[-1]:.2f}%")
                
                print("\n✅ Trading strategies analysis completed successfully!")
                print("✅ CSV files automatically exported to data/ folder:")
                print("   - trading_strategies_comparison.csv")
                print("   - stock_weights.csv")
                
            else:
                print("❌ Failed to generate strategy comparison data")
                
        except Exception as generation_error:
            print(f"❌ Error generating fresh data: {generation_error}")
            print("💡 This script works best when run within the Streamlit app environment")
            print("💡 To run standalone, ensure all dependencies and data files are available")
