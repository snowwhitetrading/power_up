import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
import calendar
from plotly.subplots import make_subplots
import calendar
import io
import time
from datetime import datetime, timedelta
import warnings
import requests
import json
import numpy as np

# Try to import ssi_api, make it optional
try:
    from ssi_api import get_stock_data_batch, fetch_historical_price, get_quarterly_stock_data
    SSI_API_AVAILABLE = True
except ImportError:
    SSI_API_AVAILABLE = False
    st.error("❌ SSI API module not available. Real stock data cannot be fetched.")

# Import hydro strategy module
try:
    from hydro_strategy import create_portfolios, create_benchmark_portfolios
    HYDRO_STRATEGY_AVAILABLE = True
except ImportError:
    HYDRO_STRATEGY_AVAILABLE = False
    print("Warning: hydro_strategy module not available.")

# Import gas strategy module
try:
    from gas_strategy import run_gas_strategy
    GAS_STRATEGY_AVAILABLE = True
except ImportError:
    GAS_STRATEGY_AVAILABLE = False
    print("Warning: gas_strategy module not available.")

# Import coal strategy module
try:
    from coal_strategy import run_coal_strategy
    COAL_STRATEGY_AVAILABLE = True
except ImportError:
    COAL_STRATEGY_AVAILABLE = False
    print("Warning: coal_strategy module not available.")

# Import ENSO regression module
try:
    from enso_regression import (
        run_enso_regression_analysis, 
        create_oni_strategy_portfolio,
        calculate_all_power_portfolio_returns
    )
    ENSO_REGRESSION_AVAILABLE = True
except ImportError:
    ENSO_REGRESSION_AVAILABLE = False
    print("Warning: enso_regression module not available.")

# Import Company module
try:
    from power_company import render_company_tab
    COMPANY_MODULE_AVAILABLE = True
except ImportError:
    COMPANY_MODULE_AVAILABLE = False
    print("Warning: company module not available.")

# Import Strategy Results Loader (for CSV-based display)
try:
    from strategy_loader import (
        display_hydro_strategy_from_csv,
        display_gas_strategy_from_csv,
        display_coal_strategy_from_csv
    )
    STRATEGY_RESULTS_LOADER_AVAILABLE = True
except ImportError:
    STRATEGY_RESULTS_LOADER_AVAILABLE = False
    print("Warning: strategy_results_loader module not available.")

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Page configuration
st.set_page_config(page_title="Power Sector Dashboard", layout="wide")

# Title
st.title("Power Sector Dashboard")

# Helper functions
@st.cache_data
def load_vni_data():
    """Load VNI data from CSV file and convert to quarterly returns"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vni_file_path = os.path.join(script_dir, 'data',  'vn_index_monthly.csv')
        
        # Read VNI CSV
        vni_df = pd.read_csv(vni_file_path)
        
        # Clean and rename columns first
        if len(vni_df.columns) >= 2:
            # Rename the columns to standard names
            vni_df.columns = ['period', 'vnindex_value'] + list(vni_df.columns[2:])
            
            # Clean the data - remove commas from vnindex_value and convert to float
            vni_df['vnindex_value'] = vni_df['vnindex_value'].astype(str).str.replace(',', '')
            # Convert to numeric, handling any non-numeric values
            vni_df['vnindex_value'] = pd.to_numeric(vni_df['vnindex_value'], errors='coerce')
            
            # Remove rows with NaN values in vnindex_value
            vni_df = vni_df.dropna(subset=['vnindex_value'])
        else:
            st.error("VNI CSV file doesn't have enough columns")
            return pd.DataFrame()
        
        # Convert period format from "1Q2011" to "2011Q1" format
        def convert_period(period_str):
            try:
                # Skip header rows and non-period data
                if pd.isna(period_str) or str(period_str).lower() in ['date', 'period', 'time']:
                    return period_str
                
                period_str = str(period_str).strip()
                
                # Parse period like "1Q2011" -> "2011Q1"
                if 'Q' in period_str and len(period_str) > 3:
                    parts = period_str.split('Q')
                    if len(parts) == 2:
                        quarter = parts[0]  # Quarter number
                        year = parts[1]     # Full year
                        return f"{year}Q{quarter}"
                return period_str
            except Exception as e:
                print(f"Error converting period {period_str}: {e}")
                return period_str  # Return original if conversion fails
        
        vni_df['period'] = vni_df['period'].apply(convert_period)
        
        # Calculate quarterly returns
        vni_df = vni_df.sort_values('period')
        vni_df['return'] = vni_df['vnindex_value'].pct_change() * 100
        vni_df['cumulative_return'] = ((vni_df['vnindex_value'] / vni_df['vnindex_value'].iloc[0]) - 1) * 100
        
        return vni_df
        
    except Exception as e:
        st.error(f"Error loading VNI data: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def convert_df_to_excel(df, sheet_name="Data"):
    """Convert dataframe to Excel bytes for download"""
    if df is None or df.empty:
        # Create a minimal dummy dataframe to avoid Excel errors
        df = pd.DataFrame({"No Data": ["No data available"]})
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

@st.cache_data  
def convert_df_to_csv(df):
    """Convert dataframe to CSV string for download"""
    return df.to_csv(index=False).encode('utf-8')

def add_download_buttons(df, filename_prefix, container=None):
    """Add download buttons for Excel and CSV"""
    if container is None:
        container = st
    
    col1, col2 = container.columns(2)
    
    with col1:
        excel_data = convert_df_to_excel(df)
        container.download_button(
            label="📊 Download as Excel",
            data=excel_data,
            file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    with col2:
        csv_data = convert_df_to_csv(df)
        container.download_button(
            label="📄 Download as CSV", 
            data=csv_data,
            file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def calculate_ytd_growth(df, value_col, date_col, period_type):
    """Calculate proper YTD growth from cumulative values from beginning of year"""
    df = df.copy()
    df['Year'] = df[date_col].dt.year
    
    if period_type == "Monthly":
        df['Month'] = df[date_col].dt.month
        df = df.sort_values([date_col])
        
        # Calculate cumulative sum from beginning of each year
        df['Cumulative'] = df.groupby('Year')[value_col].cumsum()
        
        # For each month, get the cumulative value for same month in previous year
        df_pivot = df.pivot_table(index='Month', columns='Year', values='Cumulative', aggfunc='first')
        
        ytd_growth = []
        for _, row in df.iterrows():
            month = row['Month']
            year = row['Year']
            current_cumulative = row['Cumulative']
            
            # Get previous year's cumulative for same month
            if year-1 in df_pivot.columns and month in df_pivot.index:
                prev_year_cumulative = df_pivot.loc[month, year-1]
                if pd.notna(prev_year_cumulative) and prev_year_cumulative != 0:
                    growth = ((current_cumulative - prev_year_cumulative) / prev_year_cumulative) * 100
                    ytd_growth.append(growth)
                else:
                    ytd_growth.append(None)  # No previous year data
            else:
                ytd_growth.append(None)  # No previous year data
        
        return pd.Series(ytd_growth, index=df.index)
    
    elif period_type == "Quarterly":
        df['Quarter'] = df[date_col].dt.quarter
        df = df.sort_values([date_col])
        
        # For quarterly data, calculate cumulative within year
        df['Quarter_in_Year'] = df['Quarter']
        df['Cumulative'] = df.groupby(['Year', 'Quarter_in_Year'])[value_col].transform('first')
        
        # Calculate cumulative from Q1 to current quarter
        yearly_data = []
        for year in df['Year'].unique():
            year_df = df[df['Year'] == year].copy()
            year_df = year_df.sort_values('Quarter')
            year_df['Cumulative'] = year_df[value_col].cumsum()
            yearly_data.append(year_df)
        
        df = pd.concat(yearly_data).sort_values([date_col])
        
        # Compare with same quarter cumulative in previous year
        df_pivot = df.pivot_table(index='Quarter', columns='Year', values='Cumulative', aggfunc='first')
        
        ytd_growth = []
        for _, row in df.iterrows():
            quarter = row['Quarter']
            year = row['Year']
            current_cumulative = row['Cumulative']
            
            if year-1 in df_pivot.columns and quarter in df_pivot.index:
                prev_year_cumulative = df_pivot.loc[quarter, year-1]
                if pd.notna(prev_year_cumulative) and prev_year_cumulative != 0:
                    growth = ((current_cumulative - prev_year_cumulative) / prev_year_cumulative) * 100
                    ytd_growth.append(growth)
                else:
                    ytd_growth.append(None)
            else:
                ytd_growth.append(None)
        
        return pd.Series(ytd_growth, index=df.index)
    
    else:
        # For semi-annual and annual, YTD doesn't make much sense, return simple growth
        return df[value_col].pct_change() * 100

def calculate_yoy_growth(df, value_col, periods):
    """Calculate YoY growth only when sufficient historical data exists"""
    growth = df[value_col].pct_change(periods=periods) * 100
    
    # Set growth to NaN for periods where we don't have enough historical data
    if len(df) > periods:
        growth.iloc[:periods] = None
    else:
        growth[:] = None
        
    return growth

def update_chart_layout_with_no_secondary_grid(fig):
    """Remove gridlines from secondary y-axis while keeping the axis"""
    fig.update_layout(
        yaxis2=dict(
            showgrid=False,  # Remove secondary y-axis gridlines
            zeroline=False   # Remove zero line for secondary axis
        )
    )
    return fig

@st.cache_data(ttl=3600)  # Cache for 1 hour
# Stock Chart Functions (Mock Data for Demo)
@st.cache_data
def create_stock_performance_chart(stock_symbols, sector_name):
    """Create a stock price performance chart for year-to-date using real ssi_api data"""
    import numpy as np
    
    current_year = datetime.now().year
    start_date = f"{current_year}-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    stocks_data = {}
    successful_symbols = []
    
    # Try to get real data using ssi_api
    if SSI_API_AVAILABLE:
        try:
            # Use batch function for better performance and error handling
            stock_data_batch = get_stock_data_batch(stock_symbols, start_date, end_date)
            
            for symbol, stock_data in stock_data_batch.items():
                if stock_data is not None and not stock_data.empty and 'close' in stock_data.columns:
                    # Calculate YTD performance
                    first_price = stock_data['close'].iloc[0]
                    last_price = stock_data['close'].iloc[-1]
                    ytd_performance = ((last_price - first_price) / first_price) * 100
                    
                    stocks_data[symbol] = ytd_performance
                    successful_symbols.append(symbol)
                else:
                    print(f"No valid data returned for {symbol}")
                    
        except Exception as e:
            print(f"Error in batch stock data fetch: {e}")
            # Fallback to individual fetching
            for symbol in stock_symbols:
                try:
                    stock_data = fetch_historical_price(symbol, start_date, end_date)
                    
                    if stock_data is not None and not stock_data.empty and 'close' in stock_data.columns:
                        first_price = stock_data['close'].iloc[0]
                        last_price = stock_data['close'].iloc[-1]
                        ytd_performance = ((last_price - first_price) / first_price) * 100
                        
                        stocks_data[symbol] = ytd_performance
                        successful_symbols.append(symbol)
                    else:
                        print(f"No valid data returned for {symbol}")
                        
                except Exception as e:
                    print(f"Error fetching YTD data for {symbol}: {e}")
                    continue
    
    # If no real data available, use mock data
    if not stocks_data:
        print("Using mock data for stock performance chart")
        np.random.seed(42)  # For consistent results
        for symbol in stock_symbols:
            # Generate realistic YTD performance between -30% to +50%
            ytd_performance = np.random.normal(5, 15)  # Mean 5%, std dev 15%
            ytd_performance = max(-30, min(50, ytd_performance))  # Clamp between -30% and 50%
            stocks_data[symbol] = ytd_performance
    
    # Create bar chart
    symbols = list(stocks_data.keys())
    performances = list(stocks_data.values())
    
    # Color coding: green for positive, red for negative
    colors = ['green' if p >= 0 else 'red' for p in performances]
    
    data_source = "Real Data" if successful_symbols else "Mock Data"
    
    fig = go.Figure(data=[
        go.Bar(
            x=symbols,
            y=performances,
            marker_color=colors,
            text=[f"{p:.1f}%" for p in performances],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title=f"{sector_name} Stocks - Year-to-Date Performance ({current_year}) [{data_source}]",
        xaxis_title="Stock Symbol",
        yaxis_title="YTD Performance (%)",
        height=400,
        showlegend=False
    )
    
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    return fig

@st.cache_data
def create_weekly_cumulative_ytd_chart(stock_symbols, sector_name, frequency="Weekly", start_year=None, end_year=None, cumulative_type="YTD"):
    """Create a line chart showing cumulative returns using mock data"""
    import numpy as np
    
    # Set date range - default to 2020 to current year
    if start_year is None:
        start_year = 2020
    if end_year is None:
        end_year = datetime.now().year
    
    # Generate date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    # Create date range based on frequency
    if frequency == "Daily":
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    elif frequency == "Weekly":
        date_range = pd.date_range(start=start_date, end=end_date, freq='W')
    else:  # Monthly
        date_range = pd.date_range(start=start_date, end=end_date, freq='M')
    
    # Custom colors for the lines
    custom_colors = ['#0C4130', '#08C179', '#D3BB96', '#B78D51', '#C0C1C2', '#97999B']
    
    fig = go.Figure()
    
    # Generate mock data for each stock
    np.random.seed(42)  # For consistent results
    
    for i, symbol in enumerate(stock_symbols):
        # Generate realistic stock return data
        n_points = len(date_range)
        
        if cumulative_type == "YTD":
            # Generate YTD returns that reset each year
            returns = []
            current_return = 0
            
            for date in date_range:
                if date.month == 1 and date.day <= 7:  # Reset at beginning of year
                    current_return = 0
                
                # Add daily return (small random walk)
                daily_return = np.random.normal(0.02, 1.5)  # Small positive drift with volatility
                current_return += daily_return
                returns.append(current_return)
        else:
            # Generate cumulative returns from start
            daily_returns = np.random.normal(0.02, 1.5, n_points)  # Small positive drift
            returns = np.cumsum(daily_returns)
        
        # Get color for this stock
        color = custom_colors[i % len(custom_colors)]
        
        # Add the line trace
        return_label = "YTD Return" if cumulative_type == "YTD" else "Cumulative Return"
        fig.add_trace(go.Scatter(
            x=date_range,
            y=returns,
            mode='lines',
            name=symbol,
            line=dict(width=2, color=color),
            hovertemplate=f"{symbol}<br>Date: %{{x}}<br>{return_label}: %{{y:.2f}}%<extra></extra>"
        ))
    
    # Update layout
    return_type_label = "YTD" if cumulative_type == "YTD" else "Cumulative"
    fig.update_layout(
        title=f"{sector_name} Stocks - {frequency} {return_type_label} Returns ({start_year}-{end_year}) [Mock Data]",
        xaxis_title="Date",
        yaxis_title=f"{return_type_label} Return (%)",
        height=500,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        showlegend=True
    )
    
    # Add horizontal line at 0%
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig

@st.cache_data
def create_vnstock_chart(stock_symbols, sector_name, frequency="Weekly", start_year=2020, end_year=None):
    """Create a line chart showing cumulative returns using ssi_api data"""
    
    if end_year is None:
        end_year = datetime.now().year
    
    # Generate date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    # Custom colors for the lines
    custom_colors = ['#0C4130', '#08C179', '#D3BB96', '#B78D51', '#C0C1C2', '#97999B']
    
    fig = go.Figure()
    
    successful_symbols = []
    
    # Get data for all stocks using batch function for better performance
    if SSI_API_AVAILABLE:
        try:
            # Use batch function to get all stock data at once
            stock_data_batch = get_stock_data_batch(stock_symbols, start_date, end_date)
            
            for i, symbol in enumerate(stock_symbols):
                try:
                    stock_data = stock_data_batch.get(symbol)
                    
                    if stock_data is not None and not stock_data.empty and 'close' in stock_data.columns:
                        # Reset index to get date as a column if needed
                        if 'time' in stock_data.columns:
                            date_col = 'time'
                        else:
                            stock_data = stock_data.reset_index()
                            date_col = 'time' if 'time' in stock_data.columns else stock_data.columns[0]
                        
                        # Convert date column to datetime
                        stock_data[date_col] = pd.to_datetime(stock_data[date_col])
                        stock_data = stock_data.sort_values(date_col)
                        
                        # Calculate daily returns and cumulative returns
                        stock_data['daily_return'] = stock_data['close'].pct_change() * 100
                        stock_data['cumulative_return'] = stock_data['daily_return'].cumsum()
                        
                        # Resample based on frequency
                        stock_data_indexed = stock_data.set_index(date_col)
                        if frequency == "Weekly":
                            resampled_data = stock_data_indexed.resample('W').last()
                        elif frequency == "Monthly":
                            resampled_data = stock_data_indexed.resample('M').last()
                        else:  # Daily
                            resampled_data = stock_data_indexed
                        
                        # Get color for this stock
                        color = custom_colors[i % len(custom_colors)]
                        
                        # Add the line trace
                        fig.add_trace(go.Scatter(
                            x=resampled_data.index,
                            y=resampled_data['cumulative_return'],
                            mode='lines',
                            name=symbol,
                            line=dict(width=2, color=color),
                            hovertemplate=f"{symbol}<br>Date: %{{x}}<br>Cumulative Return: %{{y:.2f}}%<br>Close Price: %{{customdata:.2f}}<extra></extra>",
                            customdata=resampled_data['close']
                        ))
                        
                        successful_symbols.append(symbol)
                        
                except Exception as e:
                    st.warning(f"Could not process data for {symbol}: {str(e)}")
                    continue
                    
        except Exception as e:
            st.warning(f"Batch data fetch failed: {str(e)}. Falling back to individual requests.")
            # Fallback to individual requests if batch fails
            for i, symbol in enumerate(stock_symbols):
                try:
                    stock_data = fetch_historical_price(symbol, start_date, end_date)
                    
                    if stock_data is not None and not stock_data.empty and 'close' in stock_data.columns:
                        # Process individual stock data (same logic as above)
                        if 'time' in stock_data.columns:
                            date_col = 'time'
                        else:
                            stock_data = stock_data.reset_index()
                            date_col = 'time' if 'time' in stock_data.columns else stock_data.columns[0]
                        
                        stock_data[date_col] = pd.to_datetime(stock_data[date_col])
                        stock_data = stock_data.sort_values(date_col)
                        
                        stock_data['daily_return'] = stock_data['close'].pct_change() * 100
                        stock_data['cumulative_return'] = stock_data['daily_return'].cumsum()
                        
                        stock_data_indexed = stock_data.set_index(date_col)
                        if frequency == "Weekly":
                            resampled_data = stock_data_indexed.resample('W').last()
                        elif frequency == "Monthly":
                            resampled_data = stock_data_indexed.resample('M').last()
                        else:
                            resampled_data = stock_data_indexed
                        
                        color = custom_colors[i % len(custom_colors)]
                        
                        fig.add_trace(go.Scatter(
                            x=resampled_data.index,
                            y=resampled_data['cumulative_return'],
                            mode='lines',
                            name=symbol,
                            line=dict(width=2, color=color),
                            hovertemplate=f"{symbol}<br>Date: %{{x}}<br>Cumulative Return: %{{y:.2f}}%<br>Close Price: %{{customdata:.2f}}<extra></extra>",
                            customdata=resampled_data['close']
                        ))
                        
                        successful_symbols.append(symbol)
                        
                except Exception as e:
                    st.warning(f"Could not fetch data for {symbol}: {str(e)}")
                    continue
    
    # If no data was successfully retrieved, show mock data
    if not successful_symbols:
        st.warning(f"Could not fetch real data for any symbols. Showing mock data instead.")
        return create_mock_chart(stock_symbols, sector_name, frequency, start_year, end_year)
    
    # Update layout
    data_source = "SSI API (Real Data)" if successful_symbols else "Mock Data"
    symbols_text = f" ({len(successful_symbols)}/{len(stock_symbols)} symbols)" if len(successful_symbols) != len(stock_symbols) else ""
    
    fig.update_layout(
        title=f"{sector_name} Stocks - {frequency} Cumulative Returns [{data_source}]{symbols_text} ({start_year}-{end_year})",
        xaxis_title="Date",
        yaxis_title="Cumulative Return (%)",
        height=500,
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template='plotly_white'
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig

@st.cache_data
def create_mock_chart(stock_symbols, sector_name, frequency="Weekly", start_year=2020, end_year=None):
    """Create a line chart showing cumulative returns using mock data as fallback"""
    import numpy as np
    
    if end_year is None:
        end_year = datetime.now().year
    
    # Generate date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    # Create date range based on frequency
    if frequency == "Daily":
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    elif frequency == "Weekly":
        date_range = pd.date_range(start=start_date, end=end_date, freq='W')
    else:  # Monthly
        date_range = pd.date_range(start=start_date, end=end_date, freq='M')
    
    # Custom colors for the lines
    custom_colors = ['#0C4130', '#08C179', '#D3BB96', '#B78D51', '#C0C1C2', '#97999B']
    
    fig = go.Figure()
    
    # Generate mock data for each stock
    np.random.seed(42)  # For consistent results
    
    for i, symbol in enumerate(stock_symbols):
        # Generate realistic cumulative stock return data
        n_points = len(date_range)
        daily_returns = np.random.normal(0.03, 2.0, n_points)  # Slightly higher returns for long-term
        cumulative_returns = np.cumsum(daily_returns)
        
        # Get color for this stock
        color = custom_colors[i % len(custom_colors)]
        
        # Add the line trace
        fig.add_trace(go.Scatter(
            x=date_range,
            y=cumulative_returns,
            mode='lines',
            name=symbol,
            line=dict(width=2, color=color),
            hovertemplate=f"{symbol}<br>Date: %{{x}}<br>Cumulative Return: %{{y:.2f}}%<extra></extra>"
        ))
    
    # Update layout
    fig.update_layout(
        title=f"{sector_name} Stocks - {frequency} Cumulative Returns [Mock Data] ({start_year}-{end_year})",
        xaxis_title="Date",
        yaxis_title="Cumulative Return (%)",
        height=500,
        hovermode='x unified',
        showlegend=True,
        template='plotly_white'
    )
    
    # Add horizontal line at 0%
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig

@st.cache_data
def load_vinacomin_data():
    """Load and process Vinacomin commodity data"""
    try:
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vinacomin_df = pd.read_csv(os.path.join(script_dir, 'data',  'vinacomin_data_monthly.csv'))
        
        # Convert update_date to datetime
        vinacomin_df['update_date'] = pd.to_datetime(vinacomin_df['update_date'])
        
        return vinacomin_df
    except Exception as e:
        st.error(f"Error loading Vinacomin data: {e}")
        return None

# Load and process data
@st.cache_data
def load_data():
    import os
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load monthly power volume data
    df = pd.read_csv(os.path.join(script_dir, 'data',  'volume_break_monthly.csv'))
    try:
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    except:
        try:
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
        except:
            df['Date'] = pd.to_datetime(df['Date'])
    
    # Clean numeric columns - remove commas and spaces, convert to numeric
    numeric_columns = ['Hydro', 'Coals', 'Gas', 'Renewables', 'Import & Diesel']
    for col in numeric_columns:
        if col in df.columns:
            # Remove spaces and commas, then convert to numeric
            df[col] = df[col].astype(str).str.replace(',', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Quarter'] = df['Date'].dt.quarter
    df['Half'] = (df['Date'].dt.month - 1) // 6 + 1
    
    # Load renewable energy data (p_max_monthly.csv contains the target companies)
    renewable_df = None
    has_renewable_data = False
    try:
        # Load p_max_monthly.csv which contains the renewable company data we need
        renewable_df = pd.read_csv(os.path.join(script_dir, 'data',  'p_max_monthly.csv'))
        
        # Check if 'date' column exists (lowercase in p_max file), rename to 'Date'
        if 'date' in renewable_df.columns:
            renewable_df.rename(columns={'date': 'Date'}, inplace=True)
        elif 'Date' not in renewable_df.columns:
            renewable_df.rename(columns={renewable_df.columns[0]: 'Date'}, inplace=True)
        
        renewable_df['Date'] = pd.to_datetime(renewable_df['Date'])
        renewable_df['Year'] = renewable_df['Date'].dt.year
        renewable_df['Month'] = renewable_df['Date'].dt.month
        renewable_df['Quarter'] = renewable_df['Date'].dt.quarter
        renewable_df['Half'] = (renewable_df['Date'].dt.month - 1) // 6 + 1
        has_renewable_data = True
    except FileNotFoundError:
        st.warning("Renewable energy data file 'p_max_monthly.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading renewable energy data: {e}")
    
    # Load weighted average price data (CGM Price)
    cgm_df = None
    has_cgm_data = False
    try:
        cgm_df = pd.read_csv(os.path.join(script_dir, 'data',  'average_prices_monthly.csv'))
        cgm_df['date'] = pd.to_datetime(cgm_df['date'])
        cgm_df['Year'] = cgm_df['date'].dt.year
        cgm_df['Month'] = cgm_df['date'].dt.month
        cgm_df['Quarter'] = cgm_df['date'].dt.quarter
        cgm_df['Half'] = (cgm_df['date'].dt.month - 1) // 6 + 1
        has_cgm_data = True
    except FileNotFoundError:
        st.warning("CGM price data file 'average_prices_monthly.csv' not found.")
    
    # Load thermal data
    thermal_df = None
    has_thermal_data = False
    try:
        thermal_df = pd.read_csv(os.path.join(script_dir, 'data',  'thermal_cost_monthly.csv'))
        
        # Try to find date column with different possible names and check first column
        date_col = None
        
        # Check first column first (most likely to be dates)
        if len(thermal_df.columns) > 0:
            first_col = thermal_df.columns[0]
            try:
                # Test if first column can be converted to datetime
                test_dates = pd.to_datetime(thermal_df[first_col])
                date_col = first_col
            except:
                pass
        
        # If first column isn't dates, check for date-related column names
        if date_col is None:
            for col in thermal_df.columns:
                if 'date' in col.lower() or 'time' in col.lower() or 'ngay' in col.lower():
                    try:
                        test_dates = pd.to_datetime(thermal_df[col])
                        date_col = col
                        break
                    except:
                        continue
        
        if date_col:
            thermal_df['Date'] = pd.to_datetime(thermal_df[date_col])
            thermal_df['Year'] = thermal_df['Date'].dt.year
            thermal_df['Month'] = thermal_df['Date'].dt.month
            thermal_df['Quarter'] = thermal_df['Date'].dt.quarter
            thermal_df['Half'] = (thermal_df['Date'].dt.month - 1) // 6 + 1
            
            # Filter to only include data up to current month of current year
            current_date = pd.Timestamp.now()
            thermal_df = thermal_df[thermal_df['Date'] <= current_date]
        else:
            st.warning("No valid date column found in thermal data. Please check the file format.")
            thermal_df = None
        
        has_thermal_data = True
    except FileNotFoundError:
        st.warning("Thermal data file 'thermal_cost_monthly.csv' not found.")

    # Load reservoir data
    reservoir_df = None
    has_reservoir_data = False
    try:
        reservoir_df = pd.read_csv(os.path.join(script_dir, 'data',  'water_reservoir_monthly.csv'))
        # Try different date formats for flexible parsing
        try:
            reservoir_df['date_time'] = pd.to_datetime(reservoir_df['date_time'], format='%d/%m/%Y %H:%M')
        except:
            try:
                reservoir_df['date_time'] = pd.to_datetime(reservoir_df['date_time'], dayfirst=True)
            except:
                reservoir_df['date_time'] = pd.to_datetime(reservoir_df['date_time'])
        has_reservoir_data = True
            
    except FileNotFoundError:
        st.warning("Reservoir data file 'water_reservoir_monthly.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading reservoir data: {e}")
        reservoir_df = None
        has_reservoir_data = False
    
    # Load POW power data
    pow_df = None
    has_pow_data = False
    try:
        pow_df = pd.read_csv(os.path.join(script_dir, 'data',  'volume_pow_monthly.csv'))
        # Rename the first column to 'Date'
        pow_df.rename(columns={pow_df.columns[0]: 'Date'}, inplace=True)
        pow_df['Date'] = pd.to_datetime(pow_df['Date'])
        pow_df['Year'] = pow_df['Date'].dt.year
        pow_df['Month'] = pow_df['Date'].dt.month
        pow_df['Quarter'] = pow_df['Date'].dt.quarter
        pow_df['Half'] = (pow_df['Date'].dt.month - 1) // 6 + 1
        has_pow_data = True
    except FileNotFoundError:
        st.warning("POW data file 'volume_pow_monthly.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading POW data: {e}")

    # Load GSO power volume data
    gso_df = None
    has_gso_data = False
    try:
        gso_df = pd.read_csv(os.path.join(script_dir, 'data',  'volume_break_monthly.csv'))
        # Try to find date column and standardize
        if len(gso_df.columns) > 0:
            first_col = gso_df.columns[0]
            try:
                gso_df['Date'] = pd.to_datetime(gso_df[first_col])
                gso_df['Year'] = gso_df['Date'].dt.year
                gso_df['Month'] = gso_df['Date'].dt.month
                gso_df['Quarter'] = gso_df['Date'].dt.quarter
                gso_df['Half'] = (gso_df['Date'].dt.month - 1) // 6 + 1
                
                # Clean and convert numeric columns (remove spaces and commas, convert to float)
                numeric_cols = [col for col in gso_df.columns if col not in ['Date', 'Year', 'Month', 'Quarter', 'Half']]
                for col in numeric_cols:
                    if gso_df[col].dtype == 'object':  # String columns
                        # Remove spaces, commas and convert to numeric
                        gso_df[col] = pd.to_numeric(
                            gso_df[col].astype(str).str.replace(',', '').str.replace(' ', ''), 
                            errors='coerce'
                        )
                
                has_gso_data = True
            except Exception as e:
                st.warning(f"Error parsing GSO data dates: {e}")
    except FileNotFoundError:
        st.warning("GSO data file 'volume_break_monthly.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading GSO data: {e}")

    # Load can price data
    can_df = None
    has_can_data = False
    try:
        can_df = pd.read_csv(os.path.join(script_dir, 'data',  'can_price_annually.csv'))
        # Add Year column if it doesn't exist (assuming first column contains year data)
        if 'Year' not in can_df.columns and len(can_df.columns) > 0:
            # Check if first column looks like years
            first_col = can_df.columns[0]
            if can_df[first_col].dtype in ['int64', 'float64'] or can_df[first_col].astype(str).str.match(r'^\d{4}$').any():
                can_df['Year'] = can_df[first_col].astype(int)
        has_can_data = True
    except FileNotFoundError:
        st.warning("can price data file 'can_price_annually.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading can price data: {e}")

    # Load alpha ratio data
    alpha_df = None
    has_alpha_data = False
    try:
        alpha_df = pd.read_csv(os.path.join(script_dir, 'data',  'alpha_ratio_annually.csv'))
        has_alpha_data = True
    except FileNotFoundError:
        st.warning("Alpha ratio data file 'alpha_ratio_annually.csv' not found.")
    except Exception as e:
        st.warning(f"Error loading alpha ratio data: {e}")

    # Load GDP data (placeholder - add actual GDP data file when available)
    gdp_df = None
    has_gdp_data = False
    
    return df, renewable_df, thermal_df, cgm_df, reservoir_df, pow_df, gso_df, can_df, alpha_df, gdp_df, has_renewable_data, has_thermal_data, has_cgm_data, has_reservoir_data, has_pow_data, has_gso_data, has_can_data, has_alpha_data, has_gdp_data

# Load all data
df, renewable_df, thermal_df, cgm_df, reservoir_df, pow_df, gso_df, can_df, alpha_df, gdp_df, has_renewable_data, has_thermal_data, has_cgm_data, has_reservoir_data, has_pow_data, has_gso_data, has_can_data, has_alpha_data, has_gdp_data = load_data()

# Load elasticity data separately
@st.cache_data
def load_elasticity_data():
    """Load elasticity data from CSV file"""
    import os
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        elasticity_df = pd.read_csv(os.path.join(script_dir, 'data',  'elasticity_annually.csv'))
        return elasticity_df
    except FileNotFoundError:
        st.warning("Elasticity data file 'elasticity_annually.csv' not found.")
        return None
    except Exception as e:
        st.warning(f"Error loading elasticity data: {e}")
        return None

# Load additional data
elasticity_df = load_elasticity_data()

# Load ENSO data separately
@st.cache_data
def load_enso_data():
    """Load ENSO data from CSV file"""
    import os
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        enso_df = pd.read_csv(os.path.join(script_dir, 'data',  'enso_data_quarterly.csv'))
        return enso_df
    except FileNotFoundError:
        st.warning("ENSO data file 'enso_data_quarterly.csv' not found.")
        return None
    except Exception as e:
        st.warning(f"Error loading ENSO data: {e}")
        return None

enso_df = load_enso_data()

# Sidebar Navigation - display all tabs in sidebar
st.sidebar.title("Power Sector Dashboard")
st.sidebar.markdown("---")

# Create sidebar navigation with combined tabs
page_options = ["⚡Power Industry", "💧Hydro Segment", "🪨Coal Segment", "🔥Gas Segment"]
if has_renewable_data:
    page_options.append("🌱Renewable Power")
if COMPANY_MODULE_AVAILABLE:
    page_options.append("🏢Company")
page_options.extend(["🌤️Weather", "📈 Trading Strategies"])

# Use radio buttons for navigation menu style
selected_page = st.sidebar.radio(
    "Navigation",
    page_options,
    label_visibility="collapsed"
)









# Power Industry Page (Combined Total Volume + Average Price)
if selected_page == "⚡Power Industry":
    st.header("⚡ Power Industry Analysis")
    
    # Create sub-tabs for Total Volume and Average Price
    industry_tab1, industry_tab2 = st.tabs(["⚡ Total Volume", "💲 Average Price"])
    
    with industry_tab1:
        # EVN Power Volume Chart
        st.subheader("EVN Power Volume")
    
    # EVN Controls
    evn_col1, evn_col2, evn_col3, evn_col4, evn_col5 = st.columns(5)
    
    with evn_col1:
        evn_period = st.selectbox(
            "Time Period:",
            ["Monthly", "Quarterly", "Semi-annually", "Annually"],
            index=0,
            key="evn_power_volume_period"
        )
    
    with evn_col2:
        evn_growth_type = st.selectbox(
            "Growth Type:",
            ["Year-over-Year (YoY)", "Year-to-Date (YTD)"],
            index=0,
            key="evn_power_volume_growth"
        )
    
    with evn_col3:
        evn_start_year = st.selectbox(
            "Start Year:",
            range(2019, 2026),
            index=0,
            key="evn_start_year"
        )
    
    with evn_col4:
        evn_end_year = st.selectbox(
            "End Year:",
            range(2019, 2026),
            index=6,
            key="evn_end_year"
        )
    
    with evn_col5:
        selected_power_types = st.multiselect(
            "Power Types:",
            ["Gas", "Hydro", "Coals", "Renewables", "Import & Diesel"],
            default=["Gas", "Hydro", "Coals", "Renewables", "Import & Diesel"],
            key="power_types_selection"
        )
    
    # Filter data based on period and year range
    df_year_filtered = df[(df['Year'] >= evn_start_year) & (df['Year'] <= evn_end_year)].copy()
    
    if evn_period == "Monthly":
        filtered_df = df_year_filtered[['Date', 'Gas', 'Hydro', 'Coals', 'Renewables', 'Import & Diesel']].copy()
    elif evn_period == "Quarterly":
        filtered_df = df_year_filtered.groupby(['Year', 'Quarter'])[['Gas', 'Hydro', 'Coals', 'Renewables', 'Import & Diesel']].sum().reset_index()
        filtered_df['Date'] = pd.to_datetime([f"{y}-{q*3}-01" for y, q in zip(filtered_df['Year'], filtered_df['Quarter'])])
    elif evn_period == "Semi-annually":
        filtered_df = df_year_filtered.groupby(['Year', 'Half'])[['Gas', 'Hydro', 'Coals', 'Renewables', 'Import & Diesel']].sum().reset_index()
        filtered_df['Date'] = pd.to_datetime([f"{y}-{h*6}-01" for y, h in zip(filtered_df['Year'], filtered_df['Half'])])
    else:  # Annually
        filtered_df = df_year_filtered.groupby('Year')[['Gas', 'Hydro', 'Coals', 'Renewables', 'Import & Diesel']].sum().reset_index()
        filtered_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in filtered_df['Year']])
    
    # Calculate total and growth for selected power types only
    filtered_df['Total'] = filtered_df[selected_power_types].sum(axis=1)
    
    # Ensure Total column is numeric and handle NaN values
    filtered_df['Total'] = pd.to_numeric(filtered_df['Total'], errors='coerce')
    
    # Improved growth calculations
    if evn_growth_type == "Year-over-Year (YoY)":
        periods_map = {"Monthly": 12, "Quarterly": 4, "Semi-annually": 2, "Annually": 1}
        filtered_df['Total_Growth'] = calculate_yoy_growth(filtered_df, 'Total', periods_map[evn_period])
        growth_title = "YoY Growth"
    else:
        filtered_df['Total_Growth'] = calculate_ytd_growth(filtered_df, 'Total', 'Date', evn_period)
        growth_title = "YTD Growth"
    
    # Create chart with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add stacked bars for selected power types only
    power_types = ['Gas', 'Hydro', 'Coals', 'Renewables', 'Import & Diesel']
    power_names = ['Gas Power', 'Hydro Power', 'Coal Power', 'Renewables', 'Import & Diesel']
    colors = ['#0C4130', '#08C179', '#B78D51', '#C0C1C2', '#97999B']
    
    # Create x-axis labels based on period
    if evn_period == "Monthly":
        x_labels = [d.strftime('%b %Y') for d in filtered_df['Date']]
    elif evn_period == "Quarterly":
        x_labels = [f"Q{d.quarter} {d.year}" for d in filtered_df['Date']]
    elif evn_period == "Semi-annually":
        x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in filtered_df['Date']]
    else:
        x_labels = [str(int(d.year)) for d in filtered_df['Date']]
    
    # Add stacked bars for each selected power type
    for i, (power_type, power_name) in enumerate(zip(power_types, power_names)):
        if power_type in selected_power_types:
            fig.add_trace(
                go.Bar(
                    name=power_name,
                    x=x_labels,
                    y=filtered_df[power_type],
                    marker_color=colors[i],
                    hovertemplate=f"{power_name}<br>%{{x}}<br>Volume: %{{y}} MWh<extra></extra>"
                ),
                secondary_y=False
            )
    
    # Add growth line
    fig.add_trace(
        go.Scatter(
            name=growth_title,
            x=x_labels,
            y=filtered_df['Total_Growth'],
            mode='lines+markers',
            line=dict(color='red', width=2),
            marker=dict(size=4),
            hovertemplate=f"{growth_title}<br>%{{x}}<br>Growth: %{{y:.2f}}%<extra></extra>"
        ),
        secondary_y=True
    )
    
    # Update layout
    fig.update_layout(
        title=f'{evn_period} EVN Power Volume ({evn_start_year}-{evn_end_year})',
        barmode='stack',
        hovermode='x unified',
        showlegend=True
    )
    
    fig.update_yaxes(title_text="Volume (MWh)", secondary_y=False)
    fig.update_yaxes(title_text=f"{growth_title} (%)", secondary_y=True)
    fig.update_xaxes(title_text="Date")
    
    # Remove secondary y-axis gridlines
    fig = update_chart_layout_with_no_secondary_grid(fig)
    
    st.plotly_chart(fig, use_container_width=True)

    # GSO Power Volume Chart
    if has_gso_data and gso_df is not None:
        st.subheader("GSO Power Volume")
        
        # GSO Controls
        gso_col1, gso_col2, gso_col3, gso_col4 = st.columns(4)
        
        with gso_col1:
            gso_period = st.selectbox(
                "Time Period:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                index=0,
                key="gso_power_volume_period"
            )
        
        with gso_col2:
            gso_growth_type = st.selectbox(
                "Growth Type:",
                ["Year-over-Year (YoY)", "Year-to-Date (YTD)"],
                index=0,
                key="gso_power_volume_growth"
            )
        
        with gso_col3:
            gso_start_year = st.selectbox(
                "Start Year:",
                range(2019, 2026),
                index=0,
                key="gso_start_year"
            )
        
        with gso_col4:
            gso_end_year = st.selectbox(
                "End Year:",
                range(2019, 2026),
                index=6,
                key="gso_end_year"
            )
        
        # Try to identify power volume columns in GSO data
        gso_power_cols = [col for col in gso_df.columns if 'power' in col.lower() or 'volume' in col.lower() or 'mwh' in col.lower()]
        
        if not gso_power_cols:
            # If no obvious power columns, use numeric columns (excluding Date)
            gso_power_cols = [col for col in gso_df.columns if gso_df[col].dtype in ['float64', 'int64'] and col not in ['Date', 'Year', 'Month', 'Quarter', 'Half']]
        
        if gso_power_cols:
            # Filter GSO data based on period and year range
            gso_df_year_filtered = gso_df[(gso_df['Year'] >= gso_start_year) & (gso_df['Year'] <= gso_end_year)].copy()
            
            if gso_period == "Monthly":
                gso_filtered_df = gso_df_year_filtered[['Date'] + gso_power_cols].copy()
            elif gso_period == "Quarterly":
                gso_filtered_df = gso_df_year_filtered.groupby(['Year', 'Quarter'])[gso_power_cols].sum().reset_index()
                gso_filtered_df['Date'] = pd.to_datetime([f"{y}-{q*3}-01" for y, q in zip(gso_filtered_df['Year'], gso_filtered_df['Quarter'])])
            elif gso_period == "Semi-annually":
                gso_filtered_df = gso_df_year_filtered.groupby(['Year', 'Half'])[gso_power_cols].sum().reset_index()
                gso_filtered_df['Date'] = pd.to_datetime([f"{y}-{h*6}-01" for y, h in zip(gso_filtered_df['Year'], gso_filtered_df['Half'])])
            else:  # Annually
                gso_filtered_df = gso_df_year_filtered.groupby('Year')[gso_power_cols].sum().reset_index()
                gso_filtered_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in gso_filtered_df['Year']])
            
            # Calculate total GSO volume
            gso_filtered_df['GSO_Total'] = gso_filtered_df[gso_power_cols].sum(axis=1)
            
            # Calculate GSO growth
            if gso_growth_type == "Year-over-Year (YoY)":
                periods_map = {"Monthly": 12, "Quarterly": 4, "Semi-annually": 2, "Annually": 1}
                gso_filtered_df['GSO_Growth'] = calculate_yoy_growth(gso_filtered_df, 'GSO_Total', periods_map[gso_period])
                gso_growth_title = "YoY Growth"
            else:
                gso_filtered_df['GSO_Growth'] = calculate_ytd_growth(gso_filtered_df, 'GSO_Total', 'Date', gso_period)
                gso_growth_title = "YTD Growth"
            
            # Create GSO chart
            gso_fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Create x-axis labels for GSO
            if gso_period == "Monthly":
                gso_x_labels = [d.strftime('%b %Y') for d in gso_filtered_df['Date']]
            elif gso_period == "Quarterly":
                gso_x_labels = [f"Q{d.quarter} {d.year}" for d in gso_filtered_df['Date']]
            elif gso_period == "Semi-annually":
                gso_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in gso_filtered_df['Date']]
            else:
                gso_x_labels = [str(int(d.year)) for d in gso_filtered_df['Date']]
            
            # Add stacked bars for GSO power types
            gso_colors = ['#08C179']
            for i, col in enumerate(gso_power_cols):
                gso_fig.add_trace(
                    go.Bar(
                        name=col,
                        x=gso_x_labels,
                        y=gso_filtered_df[col],
                        marker_color=gso_colors[i % len(gso_colors)],
                        hovertemplate=f"{col}<br>%{{x}}<br>Volume: %{{y}} MWh<extra></extra>"
                    ),
                    secondary_y=False
                )
            
            # Add GSO growth line
            gso_fig.add_trace(
                go.Scatter(
                    name=f"{gso_growth_title}",
                    x=gso_x_labels,
                    y=gso_filtered_df['GSO_Growth'],
                    mode='lines+markers',
                    line=dict(color='red', width=2),
                    marker=dict(size=4),
                    hovertemplate=f"{gso_growth_title}<br>%{{x}}<br>Growth: %{{y:.2f}}%<extra></extra>"
                ),
                secondary_y=True
            )
            
            # Update GSO layout
            gso_fig.update_layout(
                title=f'{gso_period} GSO Power Volume ({gso_start_year}-{gso_end_year})',
                barmode='stack',
                hovermode='x unified',
                showlegend=True
            )
            
            gso_fig.update_yaxes(title_text="Volume (MWh)", secondary_y=False)
            gso_fig.update_yaxes(title_text=f"{gso_growth_title} (%)", secondary_y=True)
            gso_fig.update_xaxes(title_text="Date")
            
            # Remove secondary y-axis gridlines
            gso_fig = update_chart_layout_with_no_secondary_grid(gso_fig)
            
            st.plotly_chart(gso_fig, use_container_width=True)
        else:
            st.warning("No power volume columns found in GSO data.")
    else:
        st.info("GSO power volume data not available.")

    # Alpha Ratio Chart
    if has_alpha_data and alpha_df is not None:
        st.subheader("Alpha Ratio")
        
        # Alpha ratio controls
        alpha_col1, alpha_col2 = st.columns(2)
        
        with alpha_col1:
            alpha_start_year = st.selectbox(
                "Start Year:",
                sorted(alpha_df['Year'].unique()),
                index=0,
                key="alpha_start_year"
            )
        
        with alpha_col2:
            alpha_end_year = st.selectbox(
                "End Year:",
                sorted(alpha_df['Year'].unique()),
                index=len(alpha_df['Year'].unique())-1,
                key="alpha_end_year"
            )
        
        # Filter alpha data
        alpha_filtered = alpha_df[(alpha_df['Year'] >= alpha_start_year) & (alpha_df['Year'] <= alpha_end_year)]
        
        # Create alpha ratio chart
        fig_alpha = go.Figure()
        
        # Add thermal alpha line
        fig_alpha.add_trace(go.Scatter(
            x=alpha_filtered['Year'],
            y=alpha_filtered['Thermial alpha'],
            mode='lines+markers',
            name='Thermal Alpha',
            line=dict(color='#B78D51', width=3),
            marker=dict(size=8),
            hovertemplate="Year: %{x}<br>Thermal Alpha: %{y}%<extra></extra>"
        ))
        
        # Add hydro alpha line
        fig_alpha.add_trace(go.Scatter(
            x=alpha_filtered['Year'],
            y=alpha_filtered['Hydro alpha'],
            mode='lines+markers',
            name='Hydro Alpha',
            line=dict(color='#08C179', width=3),
            marker=dict(size=8),
            hovertemplate="Year: %{x}<br>Hydro Alpha: %{y}%<extra></extra>"
        ))

        fig_alpha.update_layout(
            title="Alpha Ratio For Hydro And Thermal",
            xaxis_title="Year",
            yaxis_title="Alpha Ratio (%)",
            hovermode='x unified',
            showlegend=True
        )
        
        st.plotly_chart(fig_alpha, use_container_width=True)

    # Maximum Capacity Section
    if has_renewable_data and renewable_df is not None:
        st.subheader("Maximum Capacity")
        
        # Check if the Maximum Capacity commercialized column exists
        power_column = 'max_power_thuong_pham_MW'
        if power_column in renewable_df.columns:
            # Controls for Maximum Capacity chart
            max_power_col1, max_power_col2 = st.columns(2)
            
            with max_power_col1:
                max_power_period = st.selectbox(
                    "Time Period:",
                    ["Daily", "Weekly", "Monthly"],
                    index=2,
                    key="max_power_period"
                )
            
            with max_power_col2:
                # Date range selector (optional filter)
                max_power_year_filter = st.selectbox(
                    "Year Filter:",
                    ["All Years"] + sorted(renewable_df['Date'].dt.year.unique(), reverse=True),
                    index=0,
                    key="max_power_year_filter"
                )
            
            # Prepare data
            max_power_df = renewable_df.copy()
            max_power_df['Date'] = pd.to_datetime(max_power_df['Date'])  # Use 'Date' with capital D
            max_power_df = max_power_df.sort_values('Date')
            
            # Apply year filter if selected
            if max_power_year_filter != "All Years":
                max_power_df = max_power_df[max_power_df['Date'].dt.year == max_power_year_filter]
            
            # Aggregate data based on period
            if max_power_period == "Daily":
                max_power_df['period'] = max_power_df['Date'].dt.strftime('%Y-%m-%d')
                max_power_grouped = max_power_df.groupby('period')[power_column].sum().reset_index()
                max_power_grouped['Date'] = pd.to_datetime(max_power_grouped['period'])
                x_labels = max_power_grouped['period'].tolist()
            elif max_power_period == "Weekly":
                max_power_df['week'] = max_power_df['Date'].dt.isocalendar().week
                max_power_df['year'] = max_power_df['Date'].dt.year
                max_power_df['period'] = max_power_df['Date'].dt.strftime('W%V %Y')
                max_power_grouped = max_power_df.groupby(['year', 'week', 'period']).agg({
                    power_column: 'sum',
                    'Date': 'min'
                }).reset_index()
                x_labels = max_power_grouped['period'].tolist()
            else:  # Monthly
                max_power_df['month'] = max_power_df['Date'].dt.month
                max_power_df['year'] = max_power_df['Date'].dt.year
                max_power_df['period'] = max_power_df['Date'].dt.strftime('%Y-%m')
                max_power_grouped = max_power_df.groupby(['year', 'month', 'period']).agg({
                    power_column: 'sum',
                    'Date': 'min'
                }).reset_index()
                x_labels = [datetime.strptime(period, '%Y-%m').strftime('%b %Y') for period in max_power_grouped['period']]
            
            if len(max_power_grouped) > 0:
                # Create the chart
                max_power_fig = go.Figure()
                
                max_power_fig.add_trace(
                    go.Bar(
                        name='Maximum Capacity',
                        x=x_labels,
                        y=max_power_grouped[power_column],
                        marker_color='#2ca02c',
                        hovertemplate="Period: %{x}<br>Max Power: %{y:.2f} MW<extra></extra>"
                    )
                )
                
                max_power_fig.update_layout(
                    title=f'{max_power_period} Maximum Capacity Commercialized',
                    xaxis_title="Period",
                    yaxis_title="Maximum Capacity (MW)",
                    hovermode='x unified',
                    showlegend=True,
                    height=400
                )
                
                st.plotly_chart(max_power_fig, use_container_width=True)
                                  
    # Download data section - moved to end
    st.subheader("📥 Download Data")
    
    # Create download data for EVN
    evn_download_df = filtered_df[['Date'] + selected_power_types + ['Total', 'Total_Growth']].copy()
    # Recreate x_labels for download
    if evn_period == "Monthly":
        evn_x_labels = [d.strftime('%b %Y') for d in filtered_df['Date']]
    elif evn_period == "Quarterly":
        evn_x_labels = [f"Q{d.quarter} {d.year}" for d in filtered_df['Date']]
    elif evn_period == "Semi-annually":
        evn_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in filtered_df['Date']]
    else:
        evn_x_labels = [str(int(d.year)) for d in filtered_df['Date']]
    
    evn_download_df['Period_Label'] = evn_x_labels
    
    # EVN Data Download
    st.write("**EVN Volume Data**")
    col1, col2 = st.columns(2)
    with col1:
        if st.download_button(
            label="📊 Download as Excel",
            data=convert_df_to_excel(evn_download_df),
            file_name=f"evn_power_volume_{evn_period.lower()}_{evn_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}_{evn_start_year}_{evn_end_year}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"evn_volume_excel_{evn_period}_{evn_growth_type}_{evn_start_year}_{evn_end_year}"
        ):
            st.success("EVN Volume data downloaded successfully!")
    
    with col2:
        if st.download_button(
            label="📄 Download as CSV",
            data=convert_df_to_csv(evn_download_df),
            file_name=f"evn_power_volume_{evn_period.lower()}_{evn_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}_{evn_start_year}_{evn_end_year}.csv",
            mime="text/csv",
            key=f"evn_volume_csv_{evn_period}_{evn_growth_type}_{evn_start_year}_{evn_end_year}"
        ):
            st.success("EVN Volume data downloaded successfully!")
    
    # GSO Data Download
    if has_gso_data and gso_df is not None and 'gso_power_cols' in locals() and len(gso_power_cols) > 0:
        st.write("**GSO Volume Data**")
        gso_download_df = gso_filtered_df[['Date'] + gso_power_cols + ['GSO_Total', 'GSO_Growth']].copy()
        gso_download_df['Period_Label'] = gso_x_labels
        
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(gso_download_df),
                file_name=f"gso_power_volume_{gso_period.lower()}_{gso_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}_{gso_start_year}_{gso_end_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"gso_volume_excel_{gso_period}_{gso_growth_type}_{gso_start_year}_{gso_end_year}"
            ):
                st.success("GSO volume data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(gso_download_df),
                file_name=f"gso_power_volume_{gso_period.lower()}_{gso_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}_{gso_start_year}_{gso_end_year}.csv",
                mime="text/csv",
                key=f"gso_volume_csv_{gso_period}_{gso_growth_type}_{gso_start_year}_{gso_end_year}"
            ):
                st.success("GSO volume data downloaded successfully!")
    
    # Alpha Ratio Data Download
    if has_alpha_data and alpha_df is not None:
        st.write("**Alpha Ratio Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(alpha_filtered),
                file_name=f"alpha_ratio_{alpha_start_year}_{alpha_end_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"alpha_excel_{alpha_start_year}_{alpha_end_year}"
            ):
                st.success("Alpha ratio data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(alpha_filtered),
                file_name=f"alpha_ratio_{alpha_start_year}_{alpha_end_year}.csv",
                mime="text/csv",
                key=f"alpha_csv_{alpha_start_year}_{alpha_end_year}"
            ):
                st.success("Alpha ratio data downloaded successfully!")

    # P Max Data Download Section
    if has_renewable_data and renewable_df is not None:
        st.write("**Maximum Capacity (Pmax) Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(renewable_df),
                file_name=f"p_max_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="pmax_excel"
            ):
                st.success("P Max data downloaded successfully!")
            
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(renewable_df),
                file_name=f"p_max_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="pmax_csv"
            ):
                st.success("P Max data downloaded successfully!")
    else:
        st.info("P Max data not available for download.")

# Average Price Page
elif selected_page == "💲Average Price":
    st.subheader("CGM Price Analysis")
    
    if has_cgm_data and cgm_df is not None:
        # Controls
        cgm_col1, cgm_col2 = st.columns(2)
        
        with cgm_col1:
            cgm_period = st.selectbox(
                "Select time period:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                index=0,
                key="cgm_period"
            )
        
        # Filter out future years (2026 and beyond)
        current_year = pd.Timestamp.now().year
        cgm_df_filtered = cgm_df[cgm_df['Year'] <= current_year]
        
        # Filter and aggregate data
        if cgm_period == "Monthly":
            cgm_filtered_df = cgm_df_filtered.groupby(['Year', 'Month'])['weighted_avg_price'].mean().reset_index()
            cgm_filtered_df['Month_Name'] = cgm_filtered_df['Month'].apply(lambda x: calendar.month_abbr[x])
            cgm_filtered_df['Period'] = cgm_filtered_df['Month_Name']
        elif cgm_period == "Quarterly":
            cgm_filtered_df = cgm_df_filtered.groupby(['Year', 'Quarter'])['weighted_avg_price'].mean().reset_index()
            cgm_filtered_df['Period'] = cgm_filtered_df['Quarter'].apply(lambda x: f"Q{x}")
        elif cgm_period == "Semi-annually":
            cgm_filtered_df = cgm_df_filtered.groupby(['Year', 'Half'])['weighted_avg_price'].mean().reset_index()
            cgm_filtered_df['Period'] = cgm_filtered_df['Half'].apply(lambda x: f"H{x}")
        else:  # Annually
            cgm_filtered_df = cgm_df_filtered.groupby('Year')['weighted_avg_price'].mean().reset_index()
            cgm_filtered_df['Period'] = cgm_filtered_df['Year'].astype(str)
        
        # Create chart with separate lines/bars for each year
        cgm_fig = go.Figure()
        
        years = sorted(cgm_filtered_df['Year'].unique())
        colors = ['#0C4130', '#08C179', '#C0C1C2', '#97999B', '#B78D51', '#014ABD']

        if cgm_period == "Annually":
            # Use bar chart for annual data
            cgm_fig.add_trace(
                go.Bar(
                    name="Weighted Average Price",
                    x=[str(int(year)) for year in years],
                    y=[cgm_filtered_df[cgm_filtered_df['Year'] == year]['weighted_avg_price'].iloc[0] for year in years],
                    marker_color='#08C179',
                    hovertemplate="Year: %{x}<br>Weighted Avg Price: %{y:,.2f} VND/kWh<extra></extra>"
                )
            )
        else:
            # Use line chart for other periods
            for i, year in enumerate(years):
                year_data = cgm_filtered_df[cgm_filtered_df['Year'] == year]
                
                cgm_fig.add_trace(
                    go.Scatter(
                        name=str(year),
                        x=year_data['Period'],
                        y=year_data['weighted_avg_price'],
                        mode='lines+markers',
                        line=dict(color=colors[i % len(colors)], width=2),
                        marker=dict(size=6),
                        hovertemplate=f"Year: {year}<br>Period: %{{x}}<br>Weighted Avg Price: %{{y:,.2f}} VND/kWh<extra></extra>"
                    )
                )
        
        cgm_fig.update_layout(
            title=f"{cgm_period} Weighted Average Price {'Analysis' if cgm_period == 'Annually' else 'Trend'}",
            xaxis_title="Year" if cgm_period == "Annually" else "Time Period",
            yaxis_title="Weighted Average Price (VND/kWh)",
            hovermode='x unified' if cgm_period != "Annually" else 'closest'
        )
        
        st.plotly_chart(cgm_fig, use_container_width=True)
    else:
        st.warning("Weighted average price data not available.")
    
    # can Price Analysis
    if has_can_data and can_df is not None:
        st.subheader("CAN Price Analysis")
          
        # Try to find the price column
        price_column = None
        for col in can_df.columns:
            if 'price' in col.lower() or 'can' in col.lower():
                if col != 'Year':  # Exclude Year column
                    price_column = col
                    break
        
        # If no price column found, use the second column (assuming first is Year)
        if price_column is None and len(can_df.columns) > 1:
            price_column = can_df.columns[1]
        
        if price_column:
            # can price controls
            can_col1, can_col2 = st.columns(2)
            
            with can_col1:
                can_start_year = st.selectbox(
                    "Start Year:",
                    sorted(can_df['Year'].unique()),
                    index=0,
                    key="can_start_year"
                )
            
            with can_col2:
                can_end_year = st.selectbox(
                    "End Year:",
                    sorted(can_df['Year'].unique()),
                    index=len(can_df['Year'].unique())-1,
                    key="can_end_year"
                )
            
            # Filter can data
            can_filtered = can_df[(can_df['Year'] >= can_start_year) & (can_df['Year'] <= can_end_year)]
            
            # Create can price chart
            fig_can = go.Figure()
            
            # Add can price bar chart
            fig_can.add_trace(go.Bar(
                x=can_filtered['Year'],
                y=can_filtered[price_column],
                name=f'{price_column}',
                marker_color='#08C179',
                hovertemplate=f"Year: %{{x}}<br>{price_column}: %{{y}}<extra></extra>"
            ))
            
            fig_can.update_layout(
                title=f"{price_column} Analysis",
                xaxis_title="Year",
                yaxis_title=price_column,
                hovermode='x unified',
                showlegend=True
            )
            
            st.plotly_chart(fig_can, use_container_width=True)
        else:
            st.warning("No price column found in can data.")
    else:
        st.warning("can price data not available.")
    
    # Download data section - moved to end
    st.subheader("📥 Download Data")
    
    # CGM Price Data Download
    if has_cgm_data and cgm_df is not None:
        st.write("**CGM Price Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(cgm_filtered_df),
                file_name=f"weighted_avg_price_{cgm_period.lower()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"cgm_excel_{cgm_period}"
            ):
                st.success("Weighted average price data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(cgm_filtered_df),
                file_name=f"weighted_avg_price_{cgm_period.lower()}.csv",
                mime="text/csv",
                key=f"cgm_csv_{cgm_period}"
            ):
                st.success("Weighted average price data downloaded successfully!")
    
    # CAN Price Data Download
    if has_can_data and can_df is not None:
        st.write("**CAN Price Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(can_filtered),
                file_name=f"can_price_{can_start_year}_{can_end_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"can_excel_{can_start_year}_{can_end_year}"
            ):
                st.success("can price data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(can_filtered),
                file_name=f"can_price_{can_start_year}_{can_end_year}.csv",
                mime="text/csv",
                key=f"can_csv_{can_start_year}_{can_end_year}"
            ):
                st.success("can price data downloaded successfully!")
    
# Hydro Segment Page (Combined Hydro Power + Hydro Strategies)
elif selected_page == "💧Hydro Segment":
    st.header("💧 Hydro Segment Analysis")
    
    # Create sub-tabs for Hydro Power and Hydro Strategies
    hydro_tab1, hydro_tab2 = st.tabs(["💧 Hydro Power", "📊 Hydro Strategies"])
    
    # Hydro Power Sub-tab
    with hydro_tab1:
        st.subheader("Hydro Power Analysis")

        # Controls for Hydro Power Volume chart
        hydro_col1, hydro_col2 = st.columns(2)
        
        with hydro_col1:
            hydro_period = st.selectbox(
                "Select Time Period:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                key="hydro_volume_period"
            )
        
        with hydro_col2:
            hydro_growth_type = st.selectbox(
                "Select Growth Type:",
                ["Year-over-Year (YoY)", "Year-to-Date (YTD)"],
                key="hydro_growth_type"
            )
        
        # Hydro Power Volume Chart
            
        # Filter hydro power data
        if hydro_period == "Monthly":
            hydro_filtered_df = df[['Date', 'Hydro']].copy()
        elif hydro_period == "Quarterly":
            hydro_filtered_df = df.groupby(['Year', 'Quarter'])['Hydro'].sum().reset_index()
            hydro_filtered_df['Date'] = pd.to_datetime([f"{y}-{q*3}-01" for y, q in zip(hydro_filtered_df['Year'], hydro_filtered_df['Quarter'])])
        elif hydro_period == "Semi-annually":
            hydro_filtered_df = df.groupby(['Year', 'Half'])['Hydro'].sum().reset_index()
            hydro_filtered_df['Date'] = pd.to_datetime([f"{y}-{h*6}-01" for y, h in zip(hydro_filtered_df['Year'], hydro_filtered_df['Half'])])
        else:  # Annually
            hydro_filtered_df = df.groupby('Year')['Hydro'].sum().reset_index()
            hydro_filtered_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in hydro_filtered_df['Year']])
    
    # Calculate growth
    if hydro_growth_type == "Year-over-Year (YoY)":
        periods_map = {"Monthly": 12, "Quarterly": 4, "Semi-annually": 2, "Annually": 1}
        hydro_filtered_df['Growth'] = calculate_yoy_growth(hydro_filtered_df, 'Hydro', periods_map[hydro_period])
        growth_title = "YoY Growth"
    else:
        hydro_filtered_df['Growth'] = calculate_ytd_growth(hydro_filtered_df, 'Hydro', 'Date', hydro_period)
        growth_title = "YTD Growth"
    
    # Create chart with secondary y-axis
    hydro_fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Create x-axis labels based on period
    if hydro_period == "Monthly":
        x_labels = [d.strftime('%b %Y') for d in hydro_filtered_df['Date']]
    elif hydro_period == "Quarterly":
        x_labels = [f"Q{d.quarter} {d.year}" for d in hydro_filtered_df['Date']]
    elif hydro_period == "Semi-annually":
        x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in hydro_filtered_df['Date']]
    else:
        x_labels = [str(int(d.year)) for d in hydro_filtered_df['Date']]
    
    hydro_fig.add_trace(
        go.Bar(
            name="Hydro Power Volume",
            x=x_labels,
            y=hydro_filtered_df['Hydro'],
            marker_color='#08C179',
            hovertemplate=f"Period: %{{x}}<br>Hydro Volume: %{{y}} MWh<extra></extra>"
        ),
        secondary_y=False
    )
    
    # Add growth line
    hydro_fig.add_trace(
        go.Scatter(
            name=growth_title,
            x=x_labels,
            y=hydro_filtered_df['Growth'],
            mode='lines+markers',
            line=dict(color='red', width=2),
            marker=dict(size=4),
            hovertemplate=f"{growth_title}<br>Period: %{{x}}<br>Growth: %{{y:.2f}}%<extra></extra>"
        ),
        secondary_y=True
    )
    
    hydro_fig.update_layout(
        title=f'{hydro_period} Hydro Power Volume Growth',
        hovermode='x unified',
        showlegend=True
    )
    
    hydro_fig.update_yaxes(title_text="Hydro Power Volume (MWh)", secondary_y=False)
    hydro_fig.update_yaxes(title_text=f"{growth_title} (%)", secondary_y=True)
    hydro_fig.update_xaxes(title_text="Date")
    
    # Remove secondary y-axis gridlines
    hydro_fig = update_chart_layout_with_no_secondary_grid(hydro_fig)
    
    st.plotly_chart(hydro_fig, use_container_width=True)
    
    # Flood Flow Comparison Chart (if reservoir data available)
    st.subheader("Flood Flow Comparison (2020-2025)")
    
    if has_reservoir_data and reservoir_df is not None:
        # Controls for capacity chart
        capacity_col1, capacity_col2 = st.columns(2)
        
        with capacity_col1:
            # Get unique regions from the data and validate
            available_regions = reservoir_df['region'].unique()
            available_regions = [r for r in available_regions if pd.notna(r) and r.strip() != '']
            
            # Enhanced region mapping with all possible regions
            region_mapping = {
                "Đông Bắc Bộ": "North East - PC1",
                "Tây Bắc Bộ": "North West - PC1, REE, NED, TBC", 
                "Bắc Trung Bộ": "North Central - HDG, HNA, CHP",
                "Nam Trung Bộ": "South Central - REE, VSH, SBA, AVC",
                "Tây Nguyên": "Central Highland - REE, VSH, GEG, GHC",
                "Đông Nam Bộ": "Southeast - TMP"
            }
            
            # Create display options for regions that exist in data
            region_options = []
            for region in available_regions:
                if region in region_mapping:
                    region_options.append(region_mapping[region])
                else:
                    # For any unmapped regions, add them directly
                    region_options.append(region)
            
            # If no mapped regions found, show error
            if not region_options:
                st.error("No valid region data found in the water reservoir file.")
                st.info("Expected regions: Tây Nguyên, Tây Bắc Bộ, Đông Bắc Bộ, Bắc Trung Bộ, Nam Trung Bộ, Đông Nam Bộ")
                hydro_region = None
            else:
                # Check for missing critical regions
                expected_regions = ["Tây Nguyên", "Tây Bắc Bộ"]
                missing_regions = []
                for region in expected_regions:
                    if region not in available_regions:
                        missing_regions.append(region)
                
                if missing_regions:
                    st.warning(f"⚠️ Missing data for regions: {', '.join(missing_regions)}. These regions should have data from 2020-2025.")
                
                hydro_region = st.selectbox(
                    "Select Region:",
                    sorted(region_options),
                    key="hydro_region_capacity"
                )
        
        with capacity_col2:
            capacity_period = st.selectbox(
                "Time Period:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                key="capacity_period"
            )
        # Only proceed if we have a valid region selection
        if hydro_region is not None:
            # Create region translation dictionary
            region_translation = {
                "Đông Bắc Bộ": "Northeast",
                "Tây Bắc Bộ": "Northwest", 
                "Bắc Trung Bộ": "North Central",
                "Nam Trung Bộ": "South Central",
                "Tây Nguyên": "Central Highlands",
                "Đông Nam Bộ": "Southeast"
            }
            
            # Reverse mapping for display to original
            display_to_original = {
                "North East - PC1": "Đông Bắc Bộ",
                "North West - PC1, REE, NED, TBC": "Tây Bắc Bộ", 
                "North Central - HDG, HNA, CHP": "Bắc Trung Bộ",
                "South Central - REE, VSH, SBA, AVC": "Nam Trung Bộ",
                "Central Highland - REE, VSH, GEG, GHC": "Tây Nguyên",
                "Southeast - TMP": "Đông Nam Bộ"
            }
            
            # Add English region names
            reservoir_df['region_en'] = reservoir_df['region'].map(region_translation)
            
            # Process reservoir data based on selected period
            reservoir_df['Year'] = reservoir_df['date_time'].dt.year
            reservoir_df['Month'] = reservoir_df['date_time'].dt.month
            reservoir_df['Quarter'] = reservoir_df['date_time'].dt.quarter
            reservoir_df['Half'] = ((reservoir_df['date_time'].dt.month - 1) // 6) + 1
            
            # Get the original region name from the display name
            original_region = display_to_original.get(hydro_region, hydro_region)
            selected_region_en = region_translation.get(original_region, hydro_region)

            # Filter for selected region and years 2020-2025
            region_data = reservoir_df[reservoir_df['region'] == original_region].copy()
            
            # Filter for years 2020-2025 only
            region_data = region_data[
                (region_data['Year'] >= 2020) & (region_data['Year'] <= 2025)
            ].copy()
            
            # Convert flood_capacity to numeric
            region_data['flood_capacity'] = pd.to_numeric(region_data['flood_capacity'], errors='coerce')
            
            # Filter out rows with invalid flood_capacity data
            region_data = region_data[region_data['flood_capacity'].notna()].copy()
            
            # Aggregate based on selected period
            if capacity_period == "Monthly":
                capacity_comparison = region_data.groupby(['Year', 'Month'])['flood_capacity'].mean().reset_index()
                capacity_comparison['Period'] = capacity_comparison['Month']
                capacity_comparison['Period_Label'] = capacity_comparison['Month'].apply(lambda x: calendar.month_abbr[x])
            elif capacity_period == "Quarterly":
                capacity_comparison = region_data.groupby(['Year', 'Quarter'])['flood_capacity'].mean().reset_index()
                capacity_comparison['Period'] = capacity_comparison['Quarter']
                capacity_comparison['Period_Label'] = capacity_comparison['Quarter'].apply(lambda x: f"Q{x}")
            elif capacity_period == "Semi-annually":
                capacity_comparison = region_data.groupby(['Year', 'Half'])['flood_capacity'].mean().reset_index()
                capacity_comparison['Period'] = capacity_comparison['Half']
                capacity_comparison['Period_Label'] = capacity_comparison['Half'].apply(lambda x: f"H{x}")
            else:  # Annually
                capacity_comparison = region_data.groupby('Year')['flood_capacity'].mean().reset_index()
                capacity_comparison['Period'] = capacity_comparison['Year']
                capacity_comparison['Period_Label'] = capacity_comparison['Year'].astype(str)
            
            # Create chart showing all years 2020-2025
            capacity_fig = go.Figure()
            
            # Color palette for different years
            year_colors = {
                2020: '#0C4130',
                2021: '#08C179', 
                2022: '#D3BB96',
                2023: '#B78D51',
                2024: '#C0C1C2',
                2025: '#97999B'
            } 
            
            # Add bars for each year (2020-2025 only)
            for year in sorted(capacity_comparison['Year'].unique()):
                if 2020 <= year <= 2025:  # Only show years 2020-2025
                    year_data = capacity_comparison[capacity_comparison['Year'] == year]
                    if len(year_data) > 0:
                        capacity_fig.add_trace(
                            go.Bar(
                                name=f"{year}",
                                x=year_data['Period_Label'],
                                y=year_data['flood_capacity'],
                                marker_color=year_colors.get(year, '#87CEEB'),
                                hovertemplate=f"{year}<br>Period: %{{x}}<br>Avg Flood Flow: %{{y:.1f}} m³/s<extra></extra>"
                            )
                        )
            
            # Update chart layout
            capacity_fig.update_layout(
                title=f"{capacity_period} Flood Flow Comparison (2020-2025) - {hydro_region}",
                xaxis_title="Period",
                yaxis_title="Average Flood Flow (m³/s)",
                hovermode='x unified',
                showlegend=True,
                barmode='group'
            )
            st.plotly_chart(capacity_fig, use_container_width=True)
        
    # Flood Level Comparison Chart
    st.subheader("Flood Level Comparison (2020-2025)")
    
    if has_reservoir_data and reservoir_df is not None:
        # Controls for flood level chart
        flood_level_col1, flood_level_col2 = st.columns(2)
        
        with flood_level_col1:
            # Reuse the same region options from capacity chart
            if 'region_options' in locals() and region_options:
                hydro_region_flood_level = st.selectbox(
                    "Select Region:",
                    sorted(region_options),
                    key="hydro_region_flood_level"
                )
            else:
                st.error("No region data available for flood level chart")
                hydro_region_flood_level = None
        
        with flood_level_col2:
            flood_level_period = st.selectbox(
                "Time Period:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                key="flood_level_period"
            )
        
        # Get the original region name from the display name
        original_region_flood_level = display_to_original.get(hydro_region_flood_level, hydro_region_flood_level)
        
        # Filter for selected region - include all available years
        region_data_flood_level = reservoir_df[reservoir_df['region'] == original_region_flood_level].copy()
        
        # Convert flood_level to numeric
        region_data_flood_level['flood_level'] = pd.to_numeric(region_data_flood_level['flood_level'], errors='coerce')
        
        # Filter out rows with invalid flood_level data
        region_data_flood_level = region_data_flood_level[region_data_flood_level['flood_level'].notna()].copy()
        
        # Aggregate based on selected period
        if flood_level_period == "Monthly":
            flood_level_comparison = region_data_flood_level.groupby(['Year', 'Month'])['flood_level'].median().reset_index()
            flood_level_comparison['Period'] = flood_level_comparison['Month']
            flood_level_comparison['Period_Label'] = flood_level_comparison['Month'].apply(lambda x: calendar.month_abbr[x])
        elif flood_level_period == "Quarterly":
            flood_level_comparison = region_data_flood_level.groupby(['Year', 'Quarter'])['flood_level'].median().reset_index()
            flood_level_comparison['Period'] = flood_level_comparison['Quarter']
            flood_level_comparison['Period_Label'] = flood_level_comparison['Quarter'].apply(lambda x: f"Q{x}")
        elif flood_level_period == "Semi-annually":
            flood_level_comparison = region_data_flood_level.groupby(['Year', 'Half'])['flood_level'].median().reset_index()
            flood_level_comparison['Period'] = flood_level_comparison['Half']
            flood_level_comparison['Period_Label'] = flood_level_comparison['Half'].apply(lambda x: f"H{x}")
        else:  # Annually
            flood_level_comparison = region_data_flood_level.groupby('Year')['flood_level'].median().reset_index()
            flood_level_comparison['Period'] = flood_level_comparison['Year']
            flood_level_comparison['Period_Label'] = flood_level_comparison['Year'].astype(str)
        
        # Create chart showing all years
        flood_level_fig = go.Figure()
        
        # Color palette for different years
        year_colors = {
            2020: '#0C4130',
            2021: '#08C179', 
            2022: '#D3BB96',
            2023: '#B78D51',
            2024: '#C0C1C2',
            2025: '#97999B'
        }
        
        # Add bars for each year
        for year in sorted(flood_level_comparison['Year'].unique()):
            year_data = flood_level_comparison[flood_level_comparison['Year'] == year]
            if len(year_data) > 0:
                flood_level_fig.add_trace(
                    go.Bar(
                        name=f"{year}",
                        x=year_data['Period_Label'],
                        y=year_data['flood_level'],
                        marker_color=year_colors.get(year, '#87CEEB'),
                        hovertemplate=f"{year}<br>Period: %{{x}}<br>Median Flood Level: %{{y:.1f}} m<extra></extra>"
                    )
                )
        
        # Update chart layout
        flood_level_fig.update_layout(
            title=f"{flood_level_period} Flood Level Comparison (2020-2025) - {hydro_region_flood_level}",
            xaxis_title="Period",
            yaxis_title="Median Flood Level (m)",
            hovermode='x unified',
            showlegend=True,
            barmode='group'
        )
        
        st.plotly_chart(flood_level_fig, use_container_width=True)
        
    # Stock Performance Chart for Hydro Sector
    st.subheader("📈 Hydro Sector Stocks - Cumulative Returns")
    
    # Stock chart controls
    hydro_stock_col1, hydro_stock_col2, hydro_stock_col3, hydro_stock_col4 = st.columns(4)
    
    with hydro_stock_col1:
        hydro_freq = st.selectbox(
            "Select frequency:",
            ["Daily", "Weekly", "Monthly"],
            index=1,  # Default to Weekly
            key="hydro_ytd_return_freq"
        )
    
    with hydro_stock_col2:
        hydro_start_year = st.selectbox(
            "Start Year:",
            range(2020, 2026),
            index=0,  # Default to 2020
            key="hydro_start_year"
        )
    
    with hydro_stock_col3:
        hydro_end_year = st.selectbox(
            "End Year:",
            range(2020, 2026),
            index=5,  # Default to 2025
            key="hydro_end_year"
        )
    
    with hydro_stock_col4:
        hydro_return_type = st.selectbox(
            "Return Type:",
            ["Cumulative", "YTD"],
            index=0,  # Default to Cumulative
            key="hydro_return_type"
        )

    hydro_stocks = ['REE','PC1','HDG','GEG','TTA','DPG','AVC','GHC','SBA','VSH','NED','TMP','HNA','SHP']

    # Use vnstock for Vietnamese stocks
    # Stock chart section with loading indicator
    st.write("**Hydro Stock Performance Chart**")
    with st.spinner("Loading hydro stock data..."):
        if hydro_return_type == "Cumulative":
            try:
                hydro_stock_fig = create_vnstock_chart(
                    hydro_stocks, "Hydro Power", hydro_freq, hydro_start_year, hydro_end_year
                )
            except:
                # Fallback to mock data with cumulative returns
                hydro_stock_fig = create_weekly_cumulative_ytd_chart(
                    hydro_stocks, "Hydro Power", hydro_freq, hydro_start_year, hydro_end_year, "Cumulative"
                )
        else:
            hydro_stock_fig = create_weekly_cumulative_ytd_chart(
                hydro_stocks, "Hydro Power", hydro_freq, hydro_start_year, hydro_end_year, "YTD"
            )
    st.plotly_chart(hydro_stock_fig, use_container_width=True)

    # Download data section - moved to end
    st.subheader("📥 Download Data")
    
    # Hydro Volume Data Download
    st.write("**Hydro Volume Data**")
    hydro_download_df = hydro_filtered_df[['Date', 'Hydro', 'Growth']].copy()
    # Create x_labels for hydro data
    if hydro_period == "Monthly":
        hydro_x_labels = [d.strftime('%b %Y') for d in hydro_filtered_df['Date']]
    elif hydro_period == "Quarterly":
        hydro_x_labels = [f"Q{d.quarter} {d.year}" for d in hydro_filtered_df['Date']]
    elif hydro_period == "Semi-annually":
        hydro_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in hydro_filtered_df['Date']]
    else:
        hydro_x_labels = [str(int(d.year)) for d in hydro_filtered_df['Date']]
    
    hydro_download_df['Period_Label'] = hydro_x_labels
    
    col1, col2 = st.columns(2)
    with col1:
        if st.download_button(
            label="📊 Download as Excel",
            data=convert_df_to_excel(hydro_download_df),
            file_name=f"hydro_power_{hydro_period.lower()}_{hydro_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"hydro_tab_excel_{hydro_period}_{hydro_growth_type}"
        ):
            st.success("Hydro volume data downloaded successfully!")
    
    with col2:
        if st.download_button(
            label="📄 Download as CSV",
            data=convert_df_to_csv(hydro_download_df),
            file_name=f"hydro_power_{hydro_period.lower()}_{hydro_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.csv",
            mime="text/csv",
            key=f"hydro_tab_csv_{hydro_period}_{hydro_growth_type}"
        ):
            st.success("Hydro volume data downloaded successfully!")
    
    # Reservoir Flood Capacity Data Download (if available)
    if has_reservoir_data and reservoir_df is not None and 'capacity_comparison' in locals():
        st.write("**Reservoir Flood Capacity Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(capacity_comparison),
                file_name=f"reservoir_flood_flow_{hydro_region.replace(' ', '_')}_{capacity_period.lower()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"hydro_tab_reservoir_excel_{hydro_region}_{capacity_period}"
            ):
                st.success("Reservoir flood flow data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(capacity_comparison),
                file_name=f"reservoir_flood_flow_{hydro_region.replace(' ', '_')}_{capacity_period.lower()}.csv",
                mime="text/csv",
                key=f"hydro_tab_reservoir_csv_{hydro_region}_{capacity_period}"
            ):
                st.success("Reservoir flood capacity data downloaded successfully!")

    # Reservoir Flood Level Data Download (if available)
    if has_reservoir_data and reservoir_df is not None and 'flood_level_comparison' in locals():
        st.write("**Reservoir Flood Level Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(flood_level_comparison),
                file_name=f"reservoir_flood_level_{hydro_region_flood_level.replace(' ', '_')}_{flood_level_period.lower()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"hydro_tab_flood_level_excel_{hydro_region_flood_level}_{flood_level_period}"
            ):
                st.success("Reservoir flood level data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(flood_level_comparison),
                file_name=f"reservoir_flood_level_{hydro_region_flood_level.replace(' ', '_')}_{flood_level_period.lower()}.csv",
                mime="text/csv",
                key=f"hydro_tab_flood_level_csv_{hydro_region_flood_level}_{flood_level_period}"
            ):
                st.success("Reservoir flood level data downloaded successfully!")
    
    # Hydro Strategies sub-tab
    with hydro_tab2:
        # Use CSV-based display if available, otherwise fall back to calculation
        if STRATEGY_RESULTS_LOADER_AVAILABLE:
            display_hydro_strategy_from_csv()
        elif HYDRO_STRATEGY_AVAILABLE:
            st.subheader("💧 Hydro Strategies")
            try:
                from hydro_strategy import run_flood_portfolio_strategy
                run_flood_portfolio_strategy(strategy_type="New Methodology", selected_quarter="2020Q2")
            except Exception as e:
                st.error(f"Error running hydro strategy: {e}")
        else:
            st.error("Hydro strategy module is not available. Please check the hydro_strategy.py file.")


# Coal Segment Page (Combined Coal Power + Coal Strategies)
elif selected_page == "🪨Coal Segment":
    st.header("🪨 Coal Segment Analysis")
    
    # Create sub-tabs for Coal Power and Coal Strategies
    coal_tab1, coal_tab2 = st.tabs(["🪨 Coal Power", "📊 Coal Strategies"])
    
    with coal_tab1:
        st.subheader("Coal-fired Power Analysis")
    st.subheader("Coal-fired Power Analysis")

    # Controls for both charts
    coal_col1, coal_col2 = st.columns(2)
    
    with coal_col1:
        coal_period = st.selectbox(
            "Select Time Period:",
            ["Monthly", "Quarterly", "Semi-annually", "Annually"],
            key="coal_period"
        )
    
    with coal_col2:
        coal_growth_type = st.selectbox(
            "Select Growth Type:",
            ["Year-over-Year (YoY)", "Year-to-Date (YTD)"],
            key="coal_growth_type"
        )
    
    # Coal Power Volume Chart
    
    # Filter coal power data
    if coal_period == "Monthly":
        coal_filtered_df = df[['Date', 'Coals']].copy()
    elif coal_period == "Quarterly":
        coal_filtered_df = df.groupby(['Year', 'Quarter'])['Coals'].sum().reset_index()
        coal_filtered_df['Date'] = pd.to_datetime([f"{y}-{q*3}-01" for y, q in zip(coal_filtered_df['Year'], coal_filtered_df['Quarter'])])
    elif coal_period == "Semi-annually":
        coal_filtered_df = df.groupby(['Year', 'Half'])['Coals'].sum().reset_index()
        coal_filtered_df['Date'] = pd.to_datetime([f"{y}-{h*6}-01" for y, h in zip(coal_filtered_df['Year'], coal_filtered_df['Half'])])
    else:  # Annually
        coal_filtered_df = df.groupby('Year')['Coals'].sum().reset_index()
        coal_filtered_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in coal_filtered_df['Year']])
    
    # Calculate growth
    if coal_growth_type == "Year-over-Year (YoY)":
        periods_map = {"Monthly": 12, "Quarterly": 4, "Semi-annually": 2, "Annually": 1}
        coal_filtered_df['Growth'] = calculate_yoy_growth(coal_filtered_df, 'Coals', periods_map[coal_period])
        growth_title = "YoY Growth"
    else:
        coal_filtered_df['Growth'] = calculate_ytd_growth(coal_filtered_df, 'Coals', 'Date', coal_period)
        growth_title = "YTD Growth"
    
    # Create chart with secondary y-axis
    coal_fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Create x-axis labels based on period
    if coal_period == "Monthly":
        x_labels = [d.strftime('%b %Y') for d in coal_filtered_df['Date']]
    elif coal_period == "Quarterly":
        x_labels = [f"Q{d.quarter} {d.year}" for d in coal_filtered_df['Date']]
    elif coal_period == "Semi-annually":
        x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in coal_filtered_df['Date']]
    else:
        x_labels = [str(int(d.year)) for d in coal_filtered_df['Date']]
    
    coal_fig.add_trace(
        go.Bar(
            name="Coal Power Volume",
            x=x_labels,
            y=coal_filtered_df['Coals'],
            marker_color='#08C179',
            hovertemplate=f"Period: %{{x}}<br>Coal Volume: %{{y}} MWh<extra></extra>"
        ),
        secondary_y=False
    )
    
    # Add growth line
    coal_fig.add_trace(
        go.Scatter(
            name=growth_title,
            x=x_labels,
            y=coal_filtered_df['Growth'],
            mode='lines+markers',
            line=dict(color='red', width=2),
            marker=dict(size=4),
            hovertemplate=f"{growth_title}<br>Period: %{{x}}<br>Growth: %{{y:.2f}}%<extra></extra>"
        ),
        secondary_y=True
    )
    
    coal_fig.update_layout(
        title=f'{coal_period} Coal Power Volume Growth',
        hovermode='x unified',
        showlegend=True
    )
    
    coal_fig.update_yaxes(title_text="Coal Power Volume (MWh)", secondary_y=False)
    coal_fig.update_yaxes(title_text=f"{growth_title} (%)", secondary_y=True)
    coal_fig.update_xaxes(title_text="Date")
    
    # Remove secondary y-axis gridlines
    coal_fig = update_chart_layout_with_no_secondary_grid(coal_fig)
    
    st.plotly_chart(coal_fig, use_container_width=True)
    
    # Coal Costs Chart (if thermal data available)
    if has_thermal_data:
        st.subheader("Coal Costs Analysis")
        
        # Enhanced controls for coal costs with date range
        coal_cost_col1, coal_cost_col2, coal_cost_col3 = st.columns(3)
        
        with coal_cost_col1:
            coal_cost_period = st.selectbox(
                "Select Period for Costs:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                index=0,
                key="coal_cost_period"
            )
        
        with coal_cost_col2:
            coal_start_year = st.selectbox(
                "Start Year:",
                options=sorted([year for year in thermal_df['Year'].unique()]),
                index=0,  # Default to first year (2019)
                key="coal_costs_start_year"
            )
        
        with coal_cost_col3:
            coal_end_year = st.selectbox(
                "End Year:",
                options=sorted([year for year in thermal_df['Year'].unique()]),
                index=len(sorted([year for year in thermal_df['Year'].unique()])) - 1,  # Default to last year (2025)
                key="coal_costs_end_year"
            )
        
        # Filter thermal data by date range
        coal_thermal_filtered = thermal_df[
            (thermal_df['Year'] >= coal_start_year) & 
            (thermal_df['Year'] <= coal_end_year)
        ].copy()
        
        # Filter and aggregate coal cost data
        if coal_cost_period == "Monthly":
            coal_cost_df = coal_thermal_filtered.groupby(['Year', 'Month'])[['coal cost for Vinh Tan (VND/ton)', 'coal cost for Mong Duong (VND/ton)']].mean().reset_index()
            coal_cost_df['Date'] = pd.to_datetime([f"{y}-{m:02d}-01" for y, m in zip(coal_cost_df['Year'], coal_cost_df['Month'])])
        elif coal_cost_period == "Quarterly":
            coal_cost_df = coal_thermal_filtered.groupby(['Year', 'Quarter'])[['coal cost for Vinh Tan (VND/ton)', 'coal cost for Mong Duong (VND/ton)']].mean().reset_index()
            coal_cost_df['Date'] = pd.to_datetime([f"{y}-{q*3:02d}-01" for y, q in zip(coal_cost_df['Year'], coal_cost_df['Quarter'])])
        elif coal_cost_period == "Semi-annually":
            coal_cost_df = coal_thermal_filtered.groupby(['Year', 'Half'])[['coal cost for Vinh Tan (VND/ton)', 'coal cost for Mong Duong (VND/ton)']].mean().reset_index()
            coal_cost_df['Date'] = pd.to_datetime([f"{y}-{h*6:02d}-01" for y, h in zip(coal_cost_df['Year'], coal_cost_df['Half'])])
        else:  # Annually
            coal_cost_df = coal_thermal_filtered.groupby('Year')[['coal cost for Vinh Tan (VND/ton)', 'coal cost for Mong Duong (VND/ton)']].mean().reset_index()
            coal_cost_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in coal_cost_df['Year']])
        
        # Create line chart (always line chart from 2019-2025)
        cost_fig = go.Figure()
        
        coal_types = ['coal cost for Vinh Tan (VND/ton)', 'coal cost for Mong Duong (VND/ton)']
        coal_names = ['Vinh Tan (Central)', 'Mong Duong (North)']
        colors = ['#08C179', '#97999B']
        
        # Always use line chart for coal costs
        for coal_idx, (coal_col, coal_name) in enumerate(zip(coal_types, coal_names)):
            cost_fig.add_trace(
                go.Scatter(
                    name=coal_name,
                    x=coal_cost_df['Date'],
                    y=coal_cost_df[coal_col],
                    mode='lines+markers',
                    line=dict(color=colors[coal_idx], width=3),
                    marker=dict(size=6),
                    hovertemplate=f"{coal_name}<br>Date: %{{x}}<br>Cost: %{{y:,.0f}} VND/ton<extra></extra>"
                )
            )
        
        # Set fixed y-axis range to prevent auto-scaling
        if len(coal_cost_df) > 0:
            y_min = 0
            y_max = coal_cost_df[coal_types].max().max() * 1.1
        else:
            y_min, y_max = 0, 1000000
        
        cost_fig.update_layout(
            title=f"{coal_cost_period} Coal Costs Analysis ({coal_start_year}-{coal_end_year})",
            xaxis_title="Date",
            yaxis_title="Coal Cost (VND/ton)",
            yaxis=dict(range=[y_min, y_max]),  # Fixed y-axis range
            hovermode='x unified',
            showlegend=True
        )
        
        st.plotly_chart(cost_fig, use_container_width=True)
         
    # Download data section for coal volume
    coal_download_df = coal_filtered_df[['Date', 'Coals', 'Growth']].copy()
    
    # Create x-axis labels for download
    if coal_period == "Monthly":
        coal_x_labels = [d.strftime('%b %Y') for d in coal_filtered_df['Date']]
    elif coal_period == "Quarterly":
        coal_x_labels = [f"Q{d.quarter} {d.year}" for d in coal_filtered_df['Date']]
    elif coal_period == "Semi-annually":
        coal_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in coal_filtered_df['Date']]
    else:
        coal_x_labels = [str(int(d.year)) for d in coal_filtered_df['Date']]
    
    coal_download_df['Period_Label'] = coal_x_labels

    # Vinacomin Coal Thermal Content Prices Chart
    st.subheader("🪨 Vinacomin Imported Coal Prices")
    
    # Load Vinacomin data
    vinacomin_data = load_vinacomin_data()
    
    if vinacomin_data is not None and len(vinacomin_data) > 0:
        # Filter for only "than nhiệt trị" (thermal coal) data from Australia and South Africa
        thermal_coal_data = vinacomin_data[vinacomin_data['commodity'].str.contains('than nhiệt trị|Than nhiệt trị', na=False, regex=True)].copy()
        
        if len(thermal_coal_data) > 0:
            # Further filter for Australia (Úc) and South Africa (Nam Phi) data
            australia_data = thermal_coal_data[thermal_coal_data['commodity'].str.contains('Úc|Newcastle', na=False, regex=True)].copy()
            south_africa_data = thermal_coal_data[thermal_coal_data['commodity'].str.contains('Nam Phi|Richard Bay', na=False, regex=True)].copy()
            
            if len(australia_data) > 0 or len(south_africa_data) > 0:
                # Controls using same pattern as coal cost chart
                vinacomin_col1, vinacomin_col2, vinacomin_col3 = st.columns(3)
                
                with vinacomin_col1:
                    vinacomin_period = st.selectbox(
                        "Select Period for Vinacomin:",
                        ["Weekly", "Monthly", "Quarterly"],
                        index=1,
                        key="vinacomin_period"
                    )
            
                with vinacomin_col2:
                    # Get available years from the full thermal coal data (not filtered)
                    thermal_coal_data['year'] = pd.to_datetime(thermal_coal_data['update_date']).dt.year
                    available_years = sorted(thermal_coal_data['year'].unique())
                    
                    vinacomin_start_year = st.selectbox(
                        "Start Year:",
                        options=available_years,
                        index=0,
                        key="vinacomin_start_year"
                    )
                
                with vinacomin_col3:
                    vinacomin_end_year = st.selectbox(
                        "End Year:",
                        options=available_years,
                        index=len(available_years) - 1,
                        key="vinacomin_end_year"
                    )
                
                # Prepare data for charting
                vinacomin_fig = go.Figure()
                colors = ['#08C179', '#97999B']
                
                # Process Australia data
                if len(australia_data) > 0:
                    # Add year column for filtering
                    australia_data['year'] = pd.to_datetime(australia_data['update_date']).dt.year
                    
                    aus_filtered = australia_data[
                        (australia_data['year'] >= vinacomin_start_year) & 
                        (australia_data['year'] <= vinacomin_end_year)
                    ].copy()
                    
                    aus_filtered['update_date'] = pd.to_datetime(aus_filtered['update_date'])
                    aus_filtered = aus_filtered.sort_values('update_date')
                    
                    # Group by period
                    if vinacomin_period == "Weekly":
                        # Use data as is (weekly)
                        grouped_aus = aus_filtered
                    elif vinacomin_period == "Monthly":
                        aus_filtered['month'] = aus_filtered['update_date'].dt.to_period('M')
                        grouped_aus = aus_filtered.groupby('month').agg({
                            'price': 'mean',
                            'update_date': 'first'
                        }).reset_index()
                        grouped_aus['update_date'] = grouped_aus['month'].dt.start_time
                    else:  # Quarterly
                        aus_filtered['quarter'] = aus_filtered['update_date'].dt.to_period('Q')
                        grouped_aus = aus_filtered.groupby('quarter').agg({
                            'price': 'mean',
                            'update_date': 'first'
                        }).reset_index()
                        grouped_aus['update_date'] = grouped_aus['quarter'].dt.start_time
                    
                    vinacomin_fig.add_trace(go.Scatter(
                        x=grouped_aus['update_date'],
                        y=grouped_aus['price'],
                        mode='lines+markers',
                        name='Australia (Newcastle)',
                        line=dict(color=colors[0], width=3),
                        marker=dict(size=6),
                        hovertemplate="Australia (Newcastle)<br>Date: %{x}<br>Price: %{y:.2f} USD/Tấn<extra></extra>"
                    ))
                
                # Process South Africa data
                if len(south_africa_data) > 0:
                    # Add year column for filtering
                    south_africa_data['year'] = pd.to_datetime(south_africa_data['update_date']).dt.year
                    
                    sa_filtered = south_africa_data[
                        (south_africa_data['year'] >= vinacomin_start_year) & 
                        (south_africa_data['year'] <= vinacomin_end_year)
                    ].copy()
                    
                    sa_filtered['update_date'] = pd.to_datetime(sa_filtered['update_date'])
                    sa_filtered = sa_filtered.sort_values('update_date')
                    
                    # Group by period
                    if vinacomin_period == "Weekly":
                        # Use data as is (weekly)
                        grouped_sa = sa_filtered
                    elif vinacomin_period == "Monthly":
                        sa_filtered['month'] = sa_filtered['update_date'].dt.to_period('M')
                        grouped_sa = sa_filtered.groupby('month').agg({
                            'price': 'mean',
                            'update_date': 'first'
                        }).reset_index()
                        grouped_sa['update_date'] = grouped_sa['month'].dt.start_time
                    else:  # Quarterly
                        sa_filtered['quarter'] = sa_filtered['update_date'].dt.to_period('Q')
                        grouped_sa = sa_filtered.groupby('quarter').agg({
                            'price': 'mean',
                            'update_date': 'first'
                        }).reset_index()
                        grouped_sa['update_date'] = grouped_sa['quarter'].dt.start_time
                    
                    vinacomin_fig.add_trace(go.Scatter(
                        x=grouped_sa['update_date'],
                        y=grouped_sa['price'],
                        mode='lines+markers',
                        name='South Africa (Richard Bay)',
                        line=dict(color=colors[1], width=3),
                        marker=dict(size=6),
                        hovertemplate="South Africa (Richard Bay)<br>Date: %{x}<br>Price: %{y:.2f} USD/Tấn<extra></extra>"
                    ))
                
                vinacomin_fig.update_layout(
                    title=f"{vinacomin_period} Thermal Coal Prices - Australia vs South Africa ({vinacomin_start_year}-{vinacomin_end_year})",
                    xaxis_title="Date",
                    yaxis_title="Price (USD/Tấn)",
                    height=500,
                    hovermode='x unified',
                    showlegend=True
                )
                
                st.plotly_chart(vinacomin_fig, use_container_width=True)
            else:
                st.warning("No thermal coal data found for Australia or South Africa in Vinacomin dataset.")
        else:
            st.warning("No thermal coal data found in Vinacomin dataset.")
    else:
        st.warning("Vinacomin commodity data not available.")

    # Stock Performance Chart for Coal Sector
    st.subheader("📈 Coal Sector Stocks - Cumulative Returns")
    
    # Stock chart controls
    coal_stock_col1, coal_stock_col2, coal_stock_col3, coal_stock_col4 = st.columns(4)
    
    with coal_stock_col1:
        coal_freq = st.selectbox(
            "Select frequency:",
            ["Daily", "Weekly", "Monthly"],
            index=1,  # Default to Weekly
            key="coal_ytd_return_freq"
        )
    
    with coal_stock_col2:
        coal_start_year = st.selectbox(
            "Start Year:",
            range(2020, 2026),
            index=0,  # Default to 2020
            key="coal_stock_start_year"
        )
    
    with coal_stock_col3:
        coal_end_year = st.selectbox(
            "End Year:",
            range(2020, 2026),
            index=5,  # Default to 2025
            key="coal_stock_end_year"
        )
    
    with coal_stock_col4:
        coal_return_type = st.selectbox(
            "Return Type:",
            ["Cumulative", "YTD"],
            index=0,  # Default to Cumulative
            key="coal_return_type"
        )
    
    coal_stocks = ['QTP', 'PPC', 'HND']
    
    # Stock chart section with loading indicator
    st.write("**Coal Stock Performance Chart**")
    with st.spinner("Loading coal stock data..."):
        # Use vnstock for Vietnamese stocks
        if coal_return_type == "Cumulative":
            try:
                coal_stock_fig = create_vnstock_chart(
                    coal_stocks, "Coal Power", coal_freq, coal_start_year, coal_end_year
                )
            except:
                # Fallback to mock data with cumulative returns
                coal_stock_fig = create_weekly_cumulative_ytd_chart(
                    coal_stocks, "Coal Power", coal_freq, coal_start_year, coal_end_year, "Cumulative"
                )
        else:
            coal_stock_fig = create_weekly_cumulative_ytd_chart(
                coal_stocks, "Coal Power", coal_freq, coal_start_year, coal_end_year, "YTD"
            )
    
    st.plotly_chart(coal_stock_fig, use_container_width=True)

    # Download data section - moved to end
    st.subheader("📥 Download Data")
    
    # Coal Volume Data Download
    st.write("**Coal Volume Data**")
    coal_download_df = coal_filtered_df[['Date', 'Coals', 'Growth']].copy()
    # Create x_labels for coal data
    if coal_period == "Monthly":
        coal_x_labels = [d.strftime('%b %Y') for d in coal_filtered_df['Date']]
    elif coal_period == "Quarterly":
        coal_x_labels = [f"Q{d.quarter} {d.year}" for d in coal_filtered_df['Date']]
    elif coal_period == "Semi-annually":
        coal_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in coal_filtered_df['Date']]
    else:
        coal_x_labels = [str(int(d.year)) for d in coal_filtered_df['Date']]
    
    coal_download_df['Period_Label'] = coal_x_labels
    
    col1, col2 = st.columns(2)
    with col1:
        if st.download_button(
            label="📊 Download as Excel",
            data=convert_df_to_excel(coal_download_df),
            file_name=f"coal_power_{coal_period.lower()}_{coal_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"coal_tab_excel_{coal_period}_{coal_growth_type}"
        ):
            st.success("Coal power data downloaded successfully!")
    
    with col2:
        if st.download_button(
            label="📄 Download as CSV",
            data=convert_df_to_csv(coal_download_df),
            file_name=f"coal_power_{coal_period.lower()}_{coal_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.csv",
            mime="text/csv",
            key=f"coal_tab_csv_{coal_period}_{coal_growth_type}"
        ):
            st.success("Coal power data downloaded successfully!")
    
    # Coal Costs Data Download (if available)
    if has_thermal_data and thermal_df is not None and 'coal_cost_df' in locals():
        st.write("**Coal Costs Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(coal_cost_df),
                file_name=f"coal_costs_{coal_cost_period.lower()}_{coal_start_year}_{coal_end_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"coal_costs_excel_{coal_cost_period}_{coal_start_year}_{coal_end_year}"
            ):
                st.success("Coal costs data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(coal_cost_df),
                file_name=f"coal_costs_{coal_cost_period.lower()}_{coal_start_year}_{coal_end_year}.csv",
                mime="text/csv",
                key=f"coal_costs_csv_{coal_cost_period}_{coal_start_year}_{coal_end_year}"
            ):
                st.success("Coal costs data downloaded successfully!")
    
    # Vinacomin Data Download will be available when download functionality is implemented
    
    # Coal Strategies sub-tab
    with coal_tab2:
        # Use CSV-based display if available, otherwise fall back to calculation
        if STRATEGY_RESULTS_LOADER_AVAILABLE:
            display_coal_strategy_from_csv()
        elif COAL_STRATEGY_AVAILABLE:
            st.subheader("⛏️ Coal Strategy")
            run_coal_strategy()
        else:
            st.error("Coal strategy module is not available. Please check the coal_strategy.py file.")

# Gas Segment Page (Combined Gas Power + Gas Strategies)
elif selected_page == "🔥Gas Segment":
    st.header("🔥 Gas Segment Analysis")
    
    # Create sub-tabs for Gas Power and Gas Strategies
    gas_tab1, gas_tab2 = st.tabs(["🔥 Gas Power", "📊 Gas Strategies"])
    
    with gas_tab1:
        st.subheader("Gas-fired Power Analysis")

    # Controls for both charts
    gas_col1, gas_col2 = st.columns(2)
    
    with gas_col1:
        gas_period = st.selectbox(
            "Select Time Period:",
            ["Monthly", "Quarterly", "Semi-annually", "Annually"],
            key="gas_period"
        )
    
    with gas_col2:
        gas_growth_type = st.selectbox(
            "Select Growth Type:",
            ["Year-over-Year (YoY)", "Year-to-Date (YTD)"],
            key="gas_growth_type"
        )
    
    # Gas Power Volume Chart
    
    # Filter gas power data
    if gas_period == "Monthly":
        gas_filtered_df = df[['Date', 'Gas']].copy()
    elif gas_period == "Quarterly":
        gas_filtered_df = df.groupby(['Year', 'Quarter'])['Gas'].sum().reset_index()
        gas_filtered_df['Date'] = pd.to_datetime([f"{y}-{q*3}-01" for y, q in zip(gas_filtered_df['Year'], gas_filtered_df['Quarter'])])
    elif gas_period == "Semi-annually":
        gas_filtered_df = df.groupby(['Year', 'Half'])['Gas'].sum().reset_index()
        gas_filtered_df['Date'] = pd.to_datetime([f"{y}-{h*6}-01" for y, h in zip(gas_filtered_df['Year'], gas_filtered_df['Half'])])
    else:  # Annually
        gas_filtered_df = df.groupby('Year')['Gas'].sum().reset_index()
        gas_filtered_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in gas_filtered_df['Year']])
    
    # Calculate growth
    if gas_growth_type == "Year-over-Year (YoY)":
        periods_map = {"Monthly": 12, "Quarterly": 4, "Semi-annually": 2, "Annually": 1}
        gas_filtered_df['Growth'] = calculate_yoy_growth(gas_filtered_df, 'Gas', periods_map[gas_period])
        growth_title = "YoY Growth"
    else:
        gas_filtered_df['Growth'] = calculate_ytd_growth(gas_filtered_df, 'Gas', 'Date', gas_period)
        growth_title = "YTD Growth"
    
    # Create chart with secondary y-axis
    gas_fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Create x-axis labels based on period
    if gas_period == "Monthly":
        x_labels = [d.strftime('%b %Y') for d in gas_filtered_df['Date']]
    elif gas_period == "Quarterly":
        x_labels = [f"Q{d.quarter} {d.year}" for d in gas_filtered_df['Date']]
    elif gas_period == "Semi-annually":
        x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in gas_filtered_df['Date']]
    else:
        x_labels = [str(int(d.year)) for d in gas_filtered_df['Date']]
    
    gas_fig.add_trace(
        go.Bar(
            name="Gas Power Volume",
            x=x_labels,
            y=gas_filtered_df['Gas'],
            marker_color='#08C179',
            hovertemplate=f"Period: %{{x}}<br>Gas Volume: %{{y}} MWh<extra></extra>"
        ),
        secondary_y=False
    )
    
    # Add growth line
    gas_fig.add_trace(
        go.Scatter(
            name=growth_title,
            x=x_labels,
            y=gas_filtered_df['Growth'],
            mode='lines+markers',
            line=dict(color='red', width=2),
            marker=dict(size=4),
            hovertemplate=f"{growth_title}<br>Period: %{{x}}<br>Growth: %{{y:.2f}}%<extra></extra>"
        ),
        secondary_y=True
    )
    
    gas_fig.update_layout(
        title=f'{gas_period} Gas Power Volume Growth',
        hovermode='x unified',
        showlegend=True
    )
    
    gas_fig.update_yaxes(title_text="Gas Power Volume (MWh)", secondary_y=False)
    gas_fig.update_yaxes(title_text=f"{growth_title} (%)", secondary_y=True)
    gas_fig.update_xaxes(title_text="Date")
    
    # Remove secondary y-axis gridlines
    gas_fig = update_chart_layout_with_no_secondary_grid(gas_fig)
    
    st.plotly_chart(gas_fig, use_container_width=True)
    
    # Gas Costs Chart (if thermal data available)
    if has_thermal_data:
        st.subheader("Gas Costs Analysis")
        
        # Enhanced controls for gas costs with date range
        gas_cost_col1, gas_cost_col2, gas_cost_col3 = st.columns(3)
        
        with gas_cost_col1:
            gas_cost_period = st.selectbox(
                "Select Period for Costs:",
                ["Monthly", "Quarterly", "Semi-annually", "Annually"],
                index=0,
                key="gas_cost_period"
            )
        
        with gas_cost_col2:
            gas_start_year = st.selectbox(
                "Start Year:",
                options=sorted([year for year in thermal_df['Year'].unique()]),
                index=0,  # Default to first year (2019)
                key="gas_costs_start_year"
            )
        
        with gas_cost_col3:
            gas_end_year = st.selectbox(
                "End Year:",
                options=sorted([year for year in thermal_df['Year'].unique()]),
                index=len(sorted([year for year in thermal_df['Year'].unique()])) - 1,  # Default to last year (2025)
                key="gas_costs_end_year"
            )
        
        # Filter thermal data by date range
        gas_thermal_filtered = thermal_df[
            (thermal_df['Year'] >= gas_start_year) & 
            (thermal_df['Year'] <= gas_end_year)
        ].copy()
        
        # Filter and aggregate gas cost data
        if gas_cost_period == "Monthly":
            gas_cost_df = gas_thermal_filtered.groupby(['Year', 'Month'])[['Phu My Gas Cost (USD/MMBTU)', 'Nhon Trach Gas Cost (USD/MMBTU)']].mean().reset_index()
            gas_cost_df['Date'] = pd.to_datetime([f"{y}-{m:02d}-01" for y, m in zip(gas_cost_df['Year'], gas_cost_df['Month'])])
        elif gas_cost_period == "Quarterly":
            gas_cost_df = gas_thermal_filtered.groupby(['Year', 'Quarter'])[['Phu My Gas Cost (USD/MMBTU)', 'Nhon Trach Gas Cost (USD/MMBTU)']].mean().reset_index()
            gas_cost_df['Date'] = pd.to_datetime([f"{y}-{q*3:02d}-01" for y, q in zip(gas_cost_df['Year'], gas_cost_df['Quarter'])])
        elif gas_cost_period == "Semi-annually":
            gas_cost_df = gas_thermal_filtered.groupby(['Year', 'Half'])[['Phu My Gas Cost (USD/MMBTU)', 'Nhon Trach Gas Cost (USD/MMBTU)']].mean().reset_index()
            gas_cost_df['Date'] = pd.to_datetime([f"{y}-{h*6:02d}-01" for y, h in zip(gas_cost_df['Year'], gas_cost_df['Half'])])
        else:  # Annually
            gas_cost_df = gas_thermal_filtered.groupby('Year')[['Phu My Gas Cost (USD/MMBTU)', 'Nhon Trach Gas Cost (USD/MMBTU)']].mean().reset_index()
            gas_cost_df['Date'] = pd.to_datetime([f"{int(y)}-01-01" for y in gas_cost_df['Year']])
        
        # Create line chart (always line chart from 2019-2025)
        cost_fig = go.Figure()
        
        gas_types = ['Phu My Gas Cost (USD/MMBTU)', 'Nhon Trach Gas Cost (USD/MMBTU)']
        gas_names = ['Phu My (South)', 'Nhon Trach (South)']
        colors = ['#08C179', '#97999B']
        
        # Always use line chart for gas costs
        for gas_idx, (gas_col, gas_name) in enumerate(zip(gas_types, gas_names)):
            cost_fig.add_trace(
                go.Scatter(
                    name=gas_name,
                    x=gas_cost_df['Date'],
                    y=gas_cost_df[gas_col],
                    mode='lines+markers',
                    line=dict(color=colors[gas_idx], width=3),
                    marker=dict(size=6),
                    hovertemplate=f"{gas_name}<br>Date: %{{x}}<br>Cost: %{{y:.2f}} USD/MMBTU<extra></extra>"
                )
            )
        
        # Set fixed y-axis range to prevent auto-scaling
        if len(gas_cost_df) > 0:
            y_min = 0
            y_max = gas_cost_df[gas_types].max().max() * 1.1
        else:
            y_min, y_max = 0, 20
        
        cost_fig.update_layout(
            title=f"{gas_cost_period} Gas Costs Analysis ({gas_start_year}-{gas_end_year})",
            xaxis_title="Date",
            yaxis_title="Gas Cost (USD/MMBTU)",
            yaxis=dict(range=[y_min, y_max]),  # Fixed y-axis range
            hovermode='x unified',
            showlegend=True
        )
        
        st.plotly_chart(cost_fig, use_container_width=True)
    else:
        st.warning("Gas costs data not available.")
    
    # Download data section for gas volume
    gas_download_df = gas_filtered_df[['Date', 'Gas', 'Growth']].copy()
    
    # Create x-axis labels for download
    if gas_period == "Monthly":
        gas_x_labels = [d.strftime('%b %Y') for d in gas_filtered_df['Date']]
    elif gas_period == "Quarterly":
        gas_x_labels = [f"Q{d.quarter} {d.year}" for d in gas_filtered_df['Date']]
    elif gas_period == "Semi-annually":
        gas_x_labels = [f"H{((d.month-1)//6)+1} {d.year}" for d in gas_filtered_df['Date']]
    else:
        gas_x_labels = [str(int(d.year)) for d in gas_filtered_df['Date']]
    
    gas_download_df['Period_Label'] = gas_x_labels

    # Stock Performance Chart for Gas Sector
    st.subheader("📈 Gas Sector Stocks - Cumulative Returns")
    
    # Stock chart controls
    gas_stock_col1, gas_stock_col2, gas_stock_col3, gas_stock_col4 = st.columns(4)
    
    with gas_stock_col1:
        gas_freq = st.selectbox(
            "Select frequency:",
            ["Daily", "Weekly", "Monthly"],
            index=1,  # Default to Weekly
            key="gas_ytd_return_freq"
        )
    
    with gas_stock_col2:
        gas_start_year = st.selectbox(
            "Start Year:",
            range(2020, 2026),
            index=0,  # Default to 2020
            key="gas_stock_start_year"
        )
    
    with gas_stock_col3:
        gas_end_year = st.selectbox(
            "End Year:",
            range(2020, 2026),
            index=5,  # Default to 2025
            key="gas_stock_end_year"
        )
    
    with gas_stock_col4:
        gas_return_type = st.selectbox(
            "Return Type:",
            ["Cumulative", "YTD"],
            index=0,  # Default to Cumulative
            key="gas_return_type"
        )

    gas_stocks = ['POW', 'NT2']

    # Stock chart section with loading indicator
    st.write("**Gas Stock Performance Chart**")
    with st.spinner("Loading gas stock data..."):
        # Use vnstock for Vietnamese stocks
        if gas_return_type == "Cumulative":
            try:
                gas_stock_fig = create_vnstock_chart(
                    gas_stocks, "Gas Power", gas_freq, gas_start_year, gas_end_year
                )
            except:
                # Fallback to mock data with cumulative returns
                gas_stock_fig = create_weekly_cumulative_ytd_chart(
                    gas_stocks, "Gas Power", gas_freq, gas_start_year, gas_end_year, "Cumulative"
                )
        else:
            gas_stock_fig = create_weekly_cumulative_ytd_chart(
                gas_stocks, "Gas Power", gas_freq, gas_start_year, gas_end_year, "YTD"
            )
    
    st.plotly_chart(gas_stock_fig, use_container_width=True)

    # Download data section - moved to end
    st.subheader("📥 Download Data")
    
    # Gas Volume Data Download
    st.write("**Gas Volume Data**")
    col1, col2 = st.columns(2)
    with col1:
        if st.download_button(
            label="📊 Download as Excel",
            data=convert_df_to_excel(gas_download_df),
            file_name=f"gas_power_{gas_period.lower()}_{gas_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"gas_tab_excel_{gas_period}_{gas_growth_type}"
        ):
            st.success("Gas volume data downloaded successfully!")
    
    with col2:
        if st.download_button(
            label="📄 Download as CSV",
            data=convert_df_to_csv(gas_download_df),
            file_name=f"gas_power_{gas_period.lower()}_{gas_growth_type.lower().replace(' ', '_').replace('(', '').replace(')', '')}.csv",
            mime="text/csv",
            key=f"gas_tab_csv_{gas_period}_{gas_growth_type}"
        ):
            st.success("Gas volume data downloaded successfully!")
    
    # Gas Costs Data Download (if available)
    if 'gas_cost_df' in locals() and len(gas_cost_df) > 0:
        st.write("**Gas Costs Data**")
        col1, col2 = st.columns(2)
        with col1:
            if st.download_button(
                label="📊 Download as Excel",
                data=convert_df_to_excel(gas_cost_df),
                file_name=f"gas_costs_{gas_cost_period.lower()}_{gas_start_year}_{gas_end_year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"gas_costs_excel_{gas_cost_period}_{gas_start_year}_{gas_end_year}"
            ):
                st.success("Gas costs data downloaded successfully!")
        
        with col2:
            if st.download_button(
                label="📄 Download as CSV",
                data=convert_df_to_csv(gas_cost_df),
                file_name=f"gas_costs_{gas_cost_period.lower()}_{gas_start_year}_{gas_end_year}.csv",
                mime="text/csv",
                key=f"gas_costs_csv_{gas_cost_period}_{gas_start_year}_{gas_end_year}"
            ):
                st.success("Gas costs data downloaded successfully!")
    
    # Gas Strategies sub-tab
    with gas_tab2:
        # Use CSV-based display if available, otherwise fall back to calculation
        if STRATEGY_RESULTS_LOADER_AVAILABLE:
            display_gas_strategy_from_csv()
        elif GAS_STRATEGY_AVAILABLE:
            st.subheader("🔥 Gas Power Plant Trading Strategy")
            
            # Strategy Description
            st.markdown("""
            ### Strategy Overview
            
            **Methodology:**
            - **Diversified Portfolio**: Dynamically allocates between POW and NT2 based on growth differentials
              - If POW growth - NT2 growth > 20%: Invest 100% in POW next quarter
              - If NT2 growth - POW growth > 20%: Invest 100% in NT2 next quarter  
              - Otherwise: Equal weight allocation (50/50)
            - **Concentrated Portfolio**: Always invests 100% in the stock with higher YoY growth
            
            """)
            
            # Create sub-tabs for gas strategy
            gas_strat_tab1, gas_strat_tab2, gas_strat_tab3 = st.tabs(["📊 Performance Chart", "📋 Portfolio Details", "📈 Volume Growth"])
            
            with gas_strat_tab1:
                run_gas_strategy(None, convert_df_to_excel, convert_df_to_csv, tab_focus="performance")
            
            with gas_strat_tab2:
                run_gas_strategy(None, convert_df_to_excel, convert_df_to_csv, tab_focus="details")
            
            with gas_strat_tab3:
                run_gas_strategy(None, convert_df_to_excel, convert_df_to_csv, tab_focus="growth")
        else:
            st.error("Gas strategy module is not available. Please check the gas_strategy.py file.")


# Renewable Power Page (if available)
elif has_renewable_data and selected_page == "🌱Renewable Power":
    st.header("🌱 Renewable Energy Analysis")
    
    if renewable_df is not None:
        try:            
            # First ensure we have the required columns
            available_columns = list(renewable_df.columns)
            
            # Try to find the correct column names by searching for patterns
            wind_col = None
            ground_solar_col = None
            rooftop_solar_col = None
            
            for col in available_columns:
                col_lower = str(col).lower()
                if 'gio' in col_lower or 'wind' in col_lower:
                    wind_col = col
                elif 'trang_trai' in col_lower or ('ground' in col_lower and 'solar' in col_lower):
                    ground_solar_col = col
                elif 'mai_thuong' in col_lower or 'thuong_pham' in col_lower or ('rooftop' in col_lower and 'solar' in col_lower):
                    rooftop_solar_col = col
            
            # Build target companies dict with actual column names
            target_companies = {}
            if wind_col:
                target_companies['Wind'] = wind_col
            if ground_solar_col:
                target_companies['Ground Solar'] = ground_solar_col
            if rooftop_solar_col:
                target_companies['Rooftop Solar'] = rooftop_solar_col
            
            if not target_companies:
                st.error(f"Could not find renewable energy columns. Available columns: {available_columns}")
                st.stop()
            
            # Check for date column with various possible names
            date_col = None
            for possible_date_col in ['Date', 'date', 'DATE', 'time', 'Time']:
                if possible_date_col in available_columns:
                    date_col = possible_date_col
                    break
            
            if date_col is None:
                st.error("No date column found. Available columns: " + str(available_columns))
                st.stop()
                
            # Filter columns for target companies only
            target_cols = []
            for company_name, col_name in target_companies.items():
                if col_name in renewable_df.columns:
                    target_cols.append(col_name)
            
            # Create mapping for display names
            display_names = {v: k for k, v in target_companies.items()}

            # Create filtered dataframe with only target companies
            analysis_cols = [date_col] + target_cols
            filtered_renewable_df = renewable_df[analysis_cols].copy()
            
            # Rename date column for consistency and add time period columns
            if date_col != 'Date':
                filtered_renewable_df = filtered_renewable_df.rename(columns={date_col: 'Date'})
            
            # Add time period columns
            filtered_renewable_df['Date'] = pd.to_datetime(filtered_renewable_df['Date'])
            filtered_renewable_df['Year'] = filtered_renewable_df['Date'].dt.year
            filtered_renewable_df['Month'] = filtered_renewable_df['Date'].dt.month
            filtered_renewable_df['Quarter'] = filtered_renewable_df['Date'].dt.quarter
            filtered_renewable_df['Half'] = filtered_renewable_df['Date'].dt.month.apply(lambda x: 1 if x <= 6 else 2)
            
            # Controls
            renewable_col1, renewable_col2, renewable_col3 = st.columns(3)
            
            with renewable_col1:
                renewable_period = st.selectbox("📅 Period Type:", ["Monthly", "Quarterly", "Semi-Annual", "Annual"], key="renewable_period_select")
                          
            with renewable_col2:
                energy_type_options = ["All Energy Types"] + list(target_companies.keys())
                selected_energy_type = st.selectbox("⚡ Energy Type:", energy_type_options, key="renewable_energy_type_select")
            
            # Use all available data instead of filtering by year range
            year_filtered = filtered_renewable_df.copy()
            
            # Group data by selected time period with proper axis matching
            if renewable_period == "Monthly":
                grouped_renewable = year_filtered.copy()
                period_label = "Month"
                date_format = '%Y-%m'
                axis_title = 'Month'
            elif renewable_period == "Quarterly":
                agg_dict = {'Date': 'first'}
                for col in target_cols:
                    agg_dict[col] = 'mean'
                grouped_renewable = year_filtered.groupby(['Year', 'Quarter']).agg(agg_dict).reset_index()
                period_label = "Quarter"
                date_format = '%Y-Q%q'
                axis_title = 'Quarter'
            elif renewable_period == "Semi-Annual":
                agg_dict = {'Date': 'first'}
                for col in target_cols:
                    agg_dict[col] = 'mean'
                grouped_renewable = year_filtered.groupby(['Year', 'Half']).agg(agg_dict).reset_index()
                period_label = "Semi-Annual"
                date_format = '%Y-H%s'
                axis_title = 'Semi-Annual Period'
            else:  # Annual
                agg_dict = {'Date': 'first'}
                for col in target_cols:
                    agg_dict[col] = 'mean'
                grouped_renewable = year_filtered.groupby('Year').agg(agg_dict).reset_index()
                period_label = "Year"
                date_format = '%Y'
                axis_title = 'Year'
            
            # Ensure we have data after grouping
            if len(grouped_renewable) == 0:
                st.warning("⚠️ No data available for the selected year range and period.")
            else:
                # Convert target columns to numeric, handling any string values
                for col in target_cols:
                    if col in grouped_renewable.columns:
                        grouped_renewable[col] = pd.to_numeric(grouped_renewable[col], errors='coerce')
                
                # Calculate total renewable capacity based on selected energy type
                if selected_energy_type == "All Energy Types":
                    # Use all target companies
                    display_cols = target_cols
                    grouped_renewable['Total_Selected'] = grouped_renewable[target_cols].sum(axis=1, skipna=True)
                    chart_title_suffix = "All Energy Types"
                else:
                    # Use only selected energy type
                    selected_col = target_companies[selected_energy_type]
                    display_cols = [selected_col]
                    grouped_renewable['Total_Selected'] = grouped_renewable[selected_col]
                    chart_title_suffix = selected_energy_type
                
                # Create proper time axis labels
                if 'Date' in grouped_renewable.columns:
                    if renewable_period == "Monthly":
                        time_labels = [d.strftime('%Y-%m') for d in grouped_renewable['Date']]
                    elif renewable_period == "Quarterly":
                        time_labels = [f"{d.year}-Q{d.quarter}" for d in grouped_renewable['Date']]
                    elif renewable_period == "Semi-Annual":
                        time_labels = [f"{d.year}-H{((d.month-1)//6)+1}" for d in grouped_renewable['Date']]
                    else:  # Annual
                        time_labels = [str(d.year) for d in grouped_renewable['Date']]
                else:
                    # Fallback to index-based labels
                    time_labels = [str(i) for i in range(len(grouped_renewable))]
                
                # Create chart
                fig = go.Figure()
                
                # Add renewable capacity bars for selected energy types
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
                for i, col in enumerate(display_cols):
                    # Use display names from mapping
                    company_name = display_names[col]
                    fig.add_trace(
                        go.Bar(
                            name=company_name,
                            x=time_labels,
                            y=grouped_renewable[col],
                            marker_color=colors[i % len(colors)],
                            hovertemplate=f'<b>{company_name}</b><br>{period_label}: %{{x}}<br>Generation: %{{y:,.1f}} mkWh<extra></extra>'
                        )
                    )
                
                fig.update_layout(
                    title=f'🌱 Renewable Generation ({renewable_period}) - {chart_title_suffix}',
                    xaxis_title=axis_title,
                    yaxis_title="Generation (mkWh)",
                    barmode='stack',
                    height=600,
                    hovermode='x unified',
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
                    
        except Exception as renewable_error:
            st.error(f"❌ Error processing renewable energy data: {str(renewable_error)}")
            
            # Enhanced debug information
            with st.expander("🔍 Debug Information"):
                st.text(f"Error details: {str(renewable_error)}")
                import traceback
                st.text(f"Traceback: {traceback.format_exc()}")
                
                # Show data structure
                try:
                    st.write("Available columns:", list(renewable_df.columns))
                    st.write("Data shape:", renewable_df.shape)
                    st.write("Data types:")
                    st.write(renewable_df.dtypes)
                    st.write("Sample data:")
                    st.dataframe(renewable_df.head())
                except:
                    st.text("Could not display debug data")
    
    else:
        st.warning("⚠️ Renewable energy data is not available.")

# Company Page (if available)
elif COMPANY_MODULE_AVAILABLE and selected_page == "🏢Company":
    render_company_tab()

# Weather Page
elif selected_page == "🌤️Weather":
    
    if enso_df is not None:
        st.subheader("Oceanic Niño Index (ONI)")
        
        # ENSO Data Analysis
        if not enso_df.empty:
            # Create Oceanic Nino Index bar chart
            # Try to find the ONI column or use the first numeric column
            oni_column = None
            for col in enso_df.columns:
                if 'oni' in col.lower() or 'nino' in col.lower() or 'index' in col.lower():
                    oni_column = col
                    break
            
            # If no specific ONI column found, use first numeric column
            if oni_column is None:
                numeric_cols = enso_df.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    oni_column = numeric_cols[0]
            
            if oni_column:
                # Add period selection controls
                enso_col1, enso_col2 = st.columns(2)
                
                with enso_col1:
                    enso_period = st.selectbox(
                        "Select Time Period:",
                        ["Quarterly", "Semi-annually", "Annually"],
                        index=0,  # Default to Quarterly
                        key="enso_period"
                    )
                
                with enso_col2:
                    # Filter by year range if available
                    if 'date' in enso_df.columns:
                        # Extract years from quarterly data (format: "1Q2011")
                        temp_df = enso_df.copy()
                        temp_df['Year'] = temp_df['date'].str.extract(r'Q(\d{4})$').astype(int)
                        available_years = sorted(temp_df['Year'].unique())
                    elif 'Quarter_Year' in enso_df.columns or ('Unnamed: 0' in enso_df.columns and 'Q' in str(enso_df['Unnamed: 0'].iloc[0])):
                        # Extract years from quarterly data
                        temp_df = enso_df.copy()
                        if 'Quarter_Year' in enso_df.columns:
                            quarterly_col = 'Quarter_Year'
                        else:
                            quarterly_col = 'Unnamed: 0'
                        temp_df['Year'] = temp_df[quarterly_col].str.extract(r'Q(\d{4})$').astype(int)
                        available_years = sorted(temp_df['Year'].unique())
                    elif 'Year' in enso_df.columns:
                        available_years = sorted(enso_df['Year'].unique())
                    else:
                        available_years = list(range(2015, 2026))  # Default range
                    
                    enso_year_filter = st.selectbox(
                        "Show recent years:",
                        ["All Years", "Last 5 years", "Last 10 years"],
                        index=0,  # Default to All Years starting from 1Q2011
                        key="enso_year_filter"
                    )
                
                st.write(f"**Oceanic Niño Index {enso_period} Chart:**")
                
                # Create ONI chart
                oni_fig = go.Figure()
                
                # Filter data based on year selection
                display_df = enso_df.copy()
                if enso_year_filter != "All Years":
                    current_year = 2025  # Current year
                    if enso_year_filter == "Last 5 years":
                        year_cutoff = current_year - 5
                    else:  # Last 10 years
                        year_cutoff = current_year - 10
                    
                    # Apply year filter based on data structure
                    if 'date' in display_df.columns:
                        display_df['Year'] = display_df['date'].str.extract(r'Q(\d{4})$').astype(int)
                        display_df = display_df[display_df['Year'] >= year_cutoff]
                    elif 'Quarter_Year' in enso_df.columns or ('Unnamed: 0' in enso_df.columns and 'Q' in str(enso_df['Unnamed: 0'].iloc[0])):
                        if 'Quarter_Year' in enso_df.columns:
                            quarterly_col = 'Quarter_Year'
                        else:
                            quarterly_col = 'Unnamed: 0'
                        display_df['Year'] = display_df[quarterly_col].str.extract(r'Q(\d{4})$').astype(int)
                        display_df = display_df[display_df['Year'] >= year_cutoff]
                    elif 'Year' in enso_df.columns:
                        display_df = display_df[display_df['Year'] >= year_cutoff]
                
                # Process data based on selected period - ensuring proper quarterly baseline
                # First, ensure we have quarterly data properly structured
                quarterly_col = None
                if 'date' in display_df.columns:
                    quarterly_col = 'date'
                elif 'Quarter_Year' in display_df.columns:
                    quarterly_col = 'Quarter_Year'
                elif 'Unnamed: 0' in display_df.columns and 'Q' in str(display_df['Unnamed: 0'].iloc[0]):
                    quarterly_col = 'Unnamed: 0'
                
                if quarterly_col:
                    # Extract year and quarter information from format like "1Q2011"
                    display_df['Quarter_Num'] = display_df[quarterly_col].str.extract(r'^(\d)Q').astype(int)
                    display_df['Year'] = display_df[quarterly_col].str.extract(r'Q(\d{4})$').astype(int)
                    
                    # Use the original column as quarter label (it's already in correct format like "1Q2011")
                    display_df['Quarter_Label'] = display_df[quarterly_col]
                    
                    if enso_period == "Quarterly":
                        # Show quarterly data with proper labels
                        x_data = display_df['Quarter_Label'].tolist()
                        y_data = display_df[oni_column].tolist()
                        x_title = "Quarter"
                    
                    elif enso_period == "Semi-annually":
                        # Semi-annual aggregation - average of quarters
                        display_df['Half'] = display_df['Quarter_Num'].apply(lambda q: 1 if q <= 2 else 2)
                        display_df['Half_Label'] = display_df['Year'].astype(str) + 'H' + display_df['Half'].astype(str)
                        
                        semi_annual_data = display_df.groupby(['Year', 'Half', 'Half_Label'])[oni_column].mean().reset_index()
                        semi_annual_data = semi_annual_data.sort_values(['Year', 'Half'])
                        x_data = semi_annual_data['Half_Label'].tolist()
                        y_data = semi_annual_data[oni_column].tolist()
                        x_title = "Half Year"
                    
                    else:  # Annually
                        # Annual aggregation - average of all quarters in each year
                        yearly_data = display_df.groupby('Year')[oni_column].mean().reset_index()
                        yearly_data = yearly_data.sort_values('Year')
                        x_data = yearly_data['Year'].astype(str).tolist()
                        y_data = yearly_data[oni_column].tolist()
                        x_title = "Year"
                
                else:
                    # Fallback for other data structures
                    if 'Date' in display_df.columns:
                        display_df['Date'] = pd.to_datetime(display_df['Date'])
                        if enso_period == "Quarterly":
                            display_df['Quarter'] = display_df['Date'].dt.to_period('Q')
                            quarterly_data = display_df.groupby('Quarter')[oni_column].mean().reset_index()
                            x_data = quarterly_data['Quarter'].astype(str)
                            y_data = quarterly_data[oni_column]
                            x_title = "Quarter"
                        elif enso_period == "Semi-annually":
                            display_df['Half_Year'] = display_df['Date'].dt.to_period('6M')
                            semi_data = display_df.groupby('Half_Year')[oni_column].mean().reset_index()
                            x_data = semi_data['Half_Year'].astype(str)
                            y_data = semi_data[oni_column]
                            x_title = "Half Year"
                        else:  # Annually
                            display_df['Year'] = display_df['Date'].dt.year
                            yearly_data = display_df.groupby('Year')[oni_column].mean().reset_index()
                            x_data = yearly_data['Year'].astype(str)
                            y_data = yearly_data[oni_column]
                            x_title = "Year"
                    else:
                        # Final fallback
                        x_data = display_df.index
                        y_data = display_df[oni_column]
                        x_title = "Period"
                
                # Create color scheme for El Niño/La Niña/Neutral classification
                def get_oni_color(val):
                    if val > 0.5:
                        return '#ff4444'  # Red for El Niño
                    elif val < -0.5:
                        return '#4444ff'  # Blue for La Niña
                    else:
                        return '#888888'  # Gray for Neutral
                
                colors = [get_oni_color(val) for val in y_data]
                
                oni_fig.add_trace(
                    go.Bar(
                        x=x_data,
                        y=y_data,
                        name="Oceanic Niño Index",
                        marker_color=colors,
                        hovertemplate=f"<b>{x_title}: %{{x}}</b><br>" +
                                    f"ONI: %{{y:.3f}}<br>" +
                                    "<extra></extra>"
                    )
                )
                
                # Add horizontal line at zero
                oni_fig.add_hline(y=0, line_dash="dash", line_color="black", 
                                line_width=1, opacity=0.7)
                
                oni_fig.update_layout(
                    title=f"Oceanic Niño Index (ONI) - {enso_period} El Niño/La Niña Events",
                    xaxis_title=x_title,
                    yaxis_title=f"ONI Value ({enso_period} Average)",
                    hovermode='x unified',
                    template='plotly_white',
                    height=500,
                    showlegend=False,
                    xaxis=dict(
                        tickangle=45 if enso_period == "Quarterly" else 0
                    )
                )
                
                # Add annotations for El Niño/La Niña/Neutral thresholds
                oni_fig.add_annotation(
                    x=0.02, y=0.98,
                    xref="paper", yref="paper",
                    text="🔴 El Niño (>0.5°C)<br>⚫ Neutral (-0.5°C to 0.5°C)<br>🔵 La Niña (<-0.5°C)",
                    showarrow=False,
                    bgcolor="white",
                    bordercolor="gray",
                    borderwidth=1,
                    font=dict(size=10)
                )
                
                st.plotly_chart(oni_fig, use_container_width=True)
                
                # Download buttons for ONI data
                st.write("**Download Data:**")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.download_button(
                        label="📊 Download as Excel",
                        data=convert_df_to_excel(enso_df),
                        file_name=f"oceanic_nino_index_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="oni_excel"
                    ):
                        st.success("Oceanic Niño Index data downloaded successfully!")
                
                with col2:
                    if st.download_button(
                        label="📄 Download as CSV",
                        data=convert_df_to_csv(enso_df),
                        file_name=f"oceanic_nino_index_data.csv",
                        mime="text/csv",
                        key="oni_csv"
                    ):
                        st.success("Oceanic Niño Index data downloaded successfully!")
            else:
                st.warning("No suitable numeric columns found for Oceanic Niño Index plotting.")
        else:
            st.warning("ENSO data is empty.")
    else:
        st.warning("ENSO data not available. Please check if 'enso_data_quarterly.csv' file exists in the data directory.")

# Trading Strategies Page
elif selected_page == "📈 Trading Strategies":
    # Import only the simple cumulative returns function from trading_strategies module
    from trading_strategies import display_simple_cumulative_returns
    
    # Display only the cumulative returns from the CSV file
    display_simple_cumulative_returns()