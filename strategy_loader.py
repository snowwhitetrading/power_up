"""
Strategy Results Loader
Load and display pre-calculated strategy results from CSV files
"""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import os


def load_vni_data():
    """Load VNI data from vn_index_monthly.csv file"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vni_file = os.path.join(script_dir, 'data', 'vn_index_monthly.csv')
        
        if os.path.exists(vni_file):
            df = pd.read_csv(vni_file)
            
            # Clean column names
            if len(df.columns) >= 2:
                df.columns = ['Date', 'VNI']
            
            # Convert date format from "1Q2020" to "2020Q1" to match strategy data
            def convert_date_format(date_str):
                try:
                    # Format is "1Q2020" -> convert to "2020Q1"
                    if 'Q' in str(date_str):
                        parts = str(date_str).split('Q')
                        quarter = parts[0]
                        year = parts[1]
                        return f"{year}Q{quarter}"
                    return date_str
                except:
                    return date_str
            
            df['Date'] = df['Date'].apply(convert_date_format)
            
            # Clean VNI values (remove commas and convert to float)
            df['VNI'] = df['VNI'].astype(str).str.replace(',', '')
            df['VNI'] = pd.to_numeric(df['VNI'], errors='coerce')
            df = df.dropna(subset=['VNI'])
            
            # Calculate quarterly returns
            df['Quarter_Return'] = df['VNI'].pct_change() * 100
            df['Cumulative_Return'] = (1 + df['Quarter_Return']/100).cumprod() * 100 - 100
            
            # Fill first quarter return with 0
            df['Quarter_Return'].fillna(0, inplace=True)
            df['Cumulative_Return'].fillna(0, inplace=True)
            
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


def calculate_equal_weight_portfolio(stock_list, period_column='period'):
    """Calculate equal weight portfolio returns from raw_stock_price.csv"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file = os.path.join(script_dir, 'data', 'raw_stock_price.csv')
        
        if not os.path.exists(csv_file):
            return pd.DataFrame()
        
        df = pd.read_csv(csv_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter by date range (2011 onwards)
        start_date = pd.to_datetime('2011-01-01')
        df = df[df['timestamp'] >= start_date].copy()
        
        # Calculate quarterly returns for each stock
        quarterly_returns = {}
        for symbol in stock_list:
            stock_df = df[df['symbol'] == symbol].copy()
            if stock_df.empty:
                continue
            
            stock_df = stock_df.sort_values('timestamp')
            stock_df['year'] = stock_df['timestamp'].dt.year
            stock_df['quarter'] = stock_df['timestamp'].dt.quarter
            stock_df['period'] = stock_df['year'].astype(str) + 'Q' + stock_df['quarter'].astype(str)
            
            quarterly_data = stock_df.groupby('period')['close'].last().reset_index()
            quarterly_data['return'] = quarterly_data['close'].pct_change() * 100
            quarterly_data['return'].fillna(0, inplace=True)
            
            for _, row in quarterly_data.iterrows():
                period = row['period']
                ret = row['return']
                if period not in quarterly_returns:
                    quarterly_returns[period] = []
                quarterly_returns[period].append(ret)
        
        # Calculate equal weighted portfolio
        result_data = []
        for period in sorted(quarterly_returns.keys()):
            returns = quarterly_returns[period]
            if returns:
                avg_return = sum(returns) / len(returns)
                result_data.append({
                    period_column: period,
                    'quarterly_return': avg_return
                })
        
        if result_data:
            result_df = pd.DataFrame(result_data)
            result_df['cumulative_return'] = (1 + result_df['quarterly_return']/100).cumprod() * 100 - 100
            return result_df
        
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()


def load_hydro_strategy_results():
    """Load hydro strategy results from CSV file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(script_dir, 'data', 'strategies_results', 'hydro_strategy_results.csv')
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        return df
    else:
        st.error(f"❌ Hydro strategy results file not found: {csv_file}")
        return pd.DataFrame()


def load_gas_strategy_results():
    """Load gas strategy results from CSV file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(script_dir, 'data', 'strategies_results', 'gas_strategy_results.csv')
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        return df
    else:
        st.error(f"❌ Gas strategy results file not found: {csv_file}")
        return pd.DataFrame()


def load_coal_strategy_results():
    """Load coal strategy results from CSV file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(script_dir, 'data', 'strategies_results', 'coal_strategy_results.csv')
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        return df
    else:
        st.error(f"❌ Coal strategy results file not found: {csv_file}")
        return pd.DataFrame()


def display_hydro_strategy_from_csv():
    """Display hydro strategy results from CSV file"""
    st.title("🌊 Hydro Strategy Analysis")
    
    st.markdown("""
    ### Strategy Overview
    
    The hydro strategy uses reservoir water levels and flood risk indicators to select optimal hydro stocks.
    
    **Portfolio Types:**
    - **Flood Level Portfolio**: Selects stocks based on flood risk (high water levels)
    - **Flood Capacity Portfolio**: Selects stocks based on reservoir capacity utilization
    
    Each portfolio contains:
    - **Liquid Stock**: High liquidity hydro stock
    - **Illiquid Stock**: Lower liquidity but strategic hydro stock
    """)
    
    # Load data
    df = load_hydro_strategy_results()
    
    if df.empty:
        st.warning("No hydro strategy data available")
        return
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Performance Chart", "📋 Portfolio Details", "💧 Water Levels"])
    
    with tab1:
        st.subheader("📈 Cumulative Returns Comparison")
        
        # Separate flood_level and flood_capacity
        flood_level_df = df[df['strategy_type'] == 'flood_level'].copy()
        flood_capacity_df = df[df['strategy_type'] == 'flood_capacity'].copy()
        
        if not flood_level_df.empty and not flood_capacity_df.empty:
            # Create performance chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=flood_level_df['period'],
                y=flood_level_df['cumulative_return'],
                mode='lines+markers',
                name='Flood Level Portfolio',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=6)
            ))
            
            fig.add_trace(go.Scatter(
                x=flood_capacity_df['period'],
                y=flood_capacity_df['cumulative_return'],
                mode='lines+markers',
                name='Flood Capacity Portfolio',
                line=dict(color='#ff7f0e', width=2),
                marker=dict(size=6)
            ))
            
            fig.update_layout(
                title='Hydro Strategy Cumulative Returns',
                xaxis_title='Period',
                yaxis_title='Cumulative Return (%)',
                height=500,
                hovermode='x unified',
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary metrics
            col1, col2 = st.columns(2)
            with col1:
                final_return_level = flood_level_df['cumulative_return'].iloc[-1]
                st.metric("Flood Level Portfolio", f"{final_return_level:.2f}%")
            
            with col2:
                final_return_capacity = flood_capacity_df['cumulative_return'].iloc[-1]
                st.metric("Flood Capacity Portfolio", f"{final_return_capacity:.2f}%")
    
    with tab2:
        st.subheader("📋 Portfolio Composition Details")
        
        # Display flood level portfolio
        st.markdown("#### 🌊 Flood Level Portfolio")
        display_df = flood_level_df[['period', 'liquid_stock', 'illiquid_stock', 'quarterly_return', 'cumulative_return', 'selection_based_on']].copy()
        display_df.columns = ['Period', 'Liquid Stock', 'Illiquid Stock', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Selection Basis']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Display flood capacity portfolio
        st.markdown("#### 🏔️ Flood Capacity Portfolio")
        display_df2 = flood_capacity_df[['period', 'liquid_stock', 'illiquid_stock', 'quarterly_return', 'cumulative_return', 'selection_based_on']].copy()
        display_df2.columns = ['Period', 'Liquid Stock', 'Illiquid Stock', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Selection Basis']
        st.dataframe(display_df2, use_container_width=True, hide_index=True)
    
    with tab3:
        st.subheader("💧 Water Reservoir Analysis")
        
        # Load water reservoir data using hydro_strategy functions
        try:
            from hydro_strategy import (
                load_water_reservoir_data, 
                load_stock_mappings, 
                calculate_quarterly_growth_data
            )
            
            # Load reservoir data and stock mappings
            reservoir_df = load_water_reservoir_data()
            mappings_result = load_stock_mappings()
            
            if isinstance(mappings_result, tuple):
                mappings = mappings_result[0]
            else:
                mappings = mappings_result
            
            # Calculate growth data
            growth_df = calculate_quarterly_growth_data(reservoir_df, mappings)
            
            if not growth_df.empty:
                # Get available periods
                available_periods = sorted(growth_df['period'].unique())
                
                if available_periods:
                    # Quarter selection control
                    selected_period = st.selectbox(
                        "Select Quarter:",
                        available_periods,
                        index=len(available_periods) - 1  # Default to most recent quarter
                    )
                    
                    # Filter data for selected period
                    period_data = growth_df[growth_df['period'] == selected_period].copy()
                    
                    if not period_data.empty:
                        # Separate flood_level and flood_capacity data
                        flood_level_data = period_data[period_data['metric_type'] == 'flood_level'].copy()
                        flood_capacity_data = period_data[period_data['metric_type'] == 'flood_capacity'].copy()
                        
                        # Get growth column (either 'yoy_growth' or 'qoq_growth')
                        growth_col = 'yoy_growth' if 'yoy_growth' in period_data.columns and period_data['yoy_growth'].notna().any() else 'qoq_growth'
                        
                        # Aggregate by stock (in case a stock has multiple reservoirs)
                        flood_level_agg = flood_level_data.groupby('stock')[growth_col].mean().reset_index()
                        flood_capacity_agg = flood_capacity_data.groupby('stock')[growth_col].mean().reset_index()
                        
                        # Sort by growth
                        flood_level_agg = flood_level_agg.sort_values(growth_col, ascending=False)
                        flood_capacity_agg = flood_capacity_agg.sort_values(growth_col, ascending=False)
                        
                        # Create two columns for the charts
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            growth_type = "YoY" if growth_col == 'yoy_growth' else "QoQ"
                            st.markdown(f"**📊 Flood Level Growth - {selected_period} ({growth_type})**")
                            
                            if not flood_level_agg.empty:
                                # Create flood level growth chart
                                fig1 = go.Figure()
                                
                                # Color bars based on positive/negative growth
                                colors1 = ['#2ca02c' if x >= 0 else '#d62728' for x in flood_level_agg[growth_col]]
                                
                                fig1.add_trace(go.Bar(
                                    x=flood_level_agg['stock'],
                                    y=flood_level_agg[growth_col],
                                    marker_color=colors1,
                                    hovertemplate='<b>%{x}</b><br>Growth: %{y:.2f}%<extra></extra>'
                                ))
                                
                                fig1.update_layout(
                                    title=f'Flood Level {growth_type} Growth (%) by Stock',
                                    xaxis_title='Stock',
                                    yaxis_title='Growth (%)',
                                    height=500,
                                    xaxis={'tickangle': 45},
                                    showlegend=False
                                )
                                
                                # Add horizontal line at zero
                                fig1.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
                                
                                st.plotly_chart(fig1, use_container_width=True)
                            else:
                                st.warning("No flood level data available for this period")
                        
                        with col2:
                            st.markdown(f"**📊 Flood Capacity Growth - {selected_period} ({growth_type})**")
                            
                            if not flood_capacity_agg.empty:
                                # Create flood capacity growth chart
                                fig2 = go.Figure()
                                
                                # Color bars based on positive/negative growth
                                colors2 = ['#2ca02c' if x >= 0 else '#d62728' for x in flood_capacity_agg[growth_col]]
                                
                                fig2.add_trace(go.Bar(
                                    x=flood_capacity_agg['stock'],
                                    y=flood_capacity_agg[growth_col],
                                    marker_color=colors2,
                                    hovertemplate='<b>%{x}</b><br>Growth: %{y:.2f}%<extra></extra>'
                                ))
                                
                                fig2.update_layout(
                                    title=f'Flood Capacity {growth_type} Growth (%) by Stock',
                                    xaxis_title='Stock',
                                    yaxis_title='Growth (%)',
                                    height=500,
                                    xaxis={'tickangle': 45},
                                    showlegend=False
                                )
                                
                                # Add horizontal line at zero
                                fig2.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
                                
                                st.plotly_chart(fig2, use_container_width=True)
                            else:
                                st.warning("No flood capacity data available for this period")
                        
                        # Display data table
                        st.markdown("**📋 Detailed Growth Data by Stock**")
                        
                        # Merge flood level and capacity data
                        combined_df = pd.merge(
                            flood_level_agg.rename(columns={growth_col: 'Flood Level Growth (%)'}),
                            flood_capacity_agg.rename(columns={growth_col: 'Flood Capacity Growth (%)'}),
                            on='stock',
                            how='outer'
                        )
                        combined_df = combined_df.rename(columns={'stock': 'Stock'})
                        combined_df = combined_df.round(2)
                        combined_df = combined_df.sort_values('Flood Level Growth (%)', ascending=False, na_position='last')
                        
                        st.dataframe(combined_df, use_container_width=True, hide_index=True)
                        
                        # Show growth type info
                        if growth_type == "QoQ":
                            st.info("📌 **QoQ (Quarter-over-Quarter)**: Growth compared to previous quarter (used for 2Q2020-1Q2021 period)")
                        else:
                            st.info("📌 **YoY (Year-over-Year)**: Growth compared to same quarter last year")
                    else:
                        st.warning(f"No growth data available for {selected_period}")
                else:
                    st.warning("No quarterly periods available in the data")
            else:
                st.warning("Unable to calculate growth data from hydro strategy")
                
        except Exception as e:
            st.error(f"Error loading water reservoir data: {e}")
            import traceback
            st.error(traceback.format_exc())


def display_gas_strategy_from_csv():
    """Display gas strategy results from CSV file"""
    st.title("⚡ Gas Strategy Analysis")
    
    st.markdown("""
    ### Strategy Overview
    
    The gas strategy selects between POW and NT2 based on contracted volume growth.
    
    **Portfolio Types:**
    - **Diversified Portfolio**: Includes both stocks when contracted volume growth difference is < 20%
    - **Concentrated Portfolio**: Invests 100% in the stock with higher contracted volume growth
    """)
    
    # Load data
    df = load_gas_strategy_results()
    
    if df.empty:
        st.warning("No gas strategy data available")
        return
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📊 Performance Chart", "📋 Portfolio Details", "📈 Volume Growth"])
    
    with tab1:
        st.subheader("📈 Cumulative Returns Comparison")
        
        # Separate portfolios
        diversified_df = df[df['strategy_type'] == 'diversified'].copy()
        concentrated_df = df[df['strategy_type'] == 'concentrated'].copy()
        
        if not diversified_df.empty and not concentrated_df.empty:
            # Create performance chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=diversified_df['quarter'],
                y=diversified_df['cumulative_return'],
                mode='lines+markers',
                name='Diversified Portfolio',
                line=dict(color='#2ca02c', width=2),
                marker=dict(size=6)
            ))
            
            fig.add_trace(go.Scatter(
                x=concentrated_df['quarter'],
                y=concentrated_df['cumulative_return'],
                mode='lines+markers',
                name='Concentrated Portfolio',
                line=dict(color='#d62728', width=2),
                marker=dict(size=6)
            ))
            
            fig.update_layout(
                title='Gas Strategy Cumulative Returns',
                xaxis_title='Quarter',
                yaxis_title='Cumulative Return (%)',
                height=500,
                hovermode='x unified',
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary metrics
            col1, col2 = st.columns(2)
            with col1:
                final_return_div = diversified_df['cumulative_return'].iloc[-1]
                st.metric("Diversified Portfolio", f"{final_return_div:.2f}%")
            
            with col2:
                final_return_con = concentrated_df['cumulative_return'].iloc[-1]
                st.metric("Concentrated Portfolio", f"{final_return_con:.2f}%")
    
    with tab2:
        st.subheader("📋 Portfolio Composition Details")
        
        # Display concentrated portfolio (usually the primary one)
        st.markdown("#### 🎯 Concentrated Portfolio")
        display_df = concentrated_df[['quarter', 'selected_stocks', 'pow_weight', 'nt2_weight', 'quarterly_return', 'cumulative_return', 'decision_basis']].copy()
        display_df.columns = ['Quarter', 'Selected Stocks', 'POW Weight (%)', 'NT2 Weight (%)', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Decision Basis']
        
        # Convert weights to percentage display
        display_df['POW Weight (%)'] = (display_df['POW Weight (%)'] * 100).round(1)
        display_df['NT2 Weight (%)'] = (display_df['NT2 Weight (%)'] * 100).round(1)
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        if not diversified_df.empty:
            st.markdown("---")
            st.markdown("#### 🔀 Diversified Portfolio")
            display_df2 = diversified_df[['quarter', 'selected_stocks', 'pow_weight', 'nt2_weight', 'quarterly_return', 'cumulative_return', 'decision_basis']].copy()
            display_df2.columns = ['Quarter', 'Selected Stocks', 'POW Weight (%)', 'NT2 Weight (%)', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Decision Basis']
            
            # Convert weights to percentage display
            display_df2['POW Weight (%)'] = (display_df2['POW Weight (%)'] * 100).round(1)
            display_df2['NT2 Weight (%)'] = (display_df2['NT2 Weight (%)'] * 100).round(1)
            
            st.dataframe(display_df2, use_container_width=True, hide_index=True)
    
    with tab3:
        st.subheader("📈 Contracted Volume Growth Analysis")
        
        # Load contracted volume data using gas_strategy methodology
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            volume_file = os.path.join(script_dir, 'data', 'volume_pow_monthly.csv')
            
            if os.path.exists(volume_file):
                # Load monthly data
                df = pd.read_csv(volume_file)
                
                # Check if Date column exists
                if 'Date' not in df.columns:
                    st.warning("Date column not found in volume_pow_monthly.csv")
                else:
                    # Convert Date to datetime
                    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
                    df['Year'] = df['Date'].dt.year
                    df['Quarter'] = df['Date'].dt.quarter
                    
                    # Filter data from 2019 onwards (1Q2019 to 3Q2025)
                    start_date = pd.to_datetime('2019-01-01')
                    end_date = pd.to_datetime('2025-09-30')
                    df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
                    
                    # Find POW Contracted and NT2 contracted volume columns
                    # Note: POW Contracted includes Ca Mau (which has 0 contracted volume from 2019-2021)
                    pow_contracted_cols = [col for col in df.columns if 'POW' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
                    nt2_cols = [col for col in df.columns if 'NT2' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
                    
                    if not nt2_cols:
                        nt2_cols = [col for col in df.columns if 'NHON TRACH 2' in str(col).upper() and 'CONTRACTED' in str(col).upper()]
                    
                    if pow_contracted_cols and nt2_cols:
                        # Use POW Contracted column directly (already includes Ca Mau)
                        pow_col = pow_contracted_cols[0]
                        nt2_col = nt2_cols[0]
                        
                        # Convert to numeric
                        df[pow_col] = pd.to_numeric(df[pow_col], errors='coerce')
                        df[nt2_col] = pd.to_numeric(df[nt2_col], errors='coerce')
                        
                        # Group by quarter and sum the volumes (monthly to quarterly)
                        quarterly_df = df.groupby(['Year', 'Quarter']).agg({
                            pow_col: 'sum',
                            nt2_col: 'sum'
                        }).reset_index()
                        
                        # Create quarter label
                        quarterly_df['Quarter_Label'] = quarterly_df.apply(lambda row: f"{int(row['Year'])}Q{int(row['Quarter'])}", axis=1)
                        
                        # Sort by year and quarter
                        quarterly_df = quarterly_df.sort_values(['Year', 'Quarter']).reset_index(drop=True)
                        
                        # Calculate YoY growth (4 quarters back)
                        quarterly_df['POW_YoY_Growth'] = quarterly_df[pow_col].pct_change(periods=4) * 100
                        quarterly_df['NT2_YoY_Growth'] = quarterly_df[nt2_col].pct_change(periods=4) * 100
                        
                        # Rename columns for clarity
                        quarterly_df = quarterly_df.rename(columns={
                            pow_col: 'POW_Contracted',
                            nt2_col: 'NT2_Contracted'
                        })
                        
                        # Filter for periods with growth data
                        growth_df = quarterly_df.dropna(subset=['POW_YoY_Growth', 'NT2_YoY_Growth'])
                        
                        if not growth_df.empty:
                            # Create comparison chart
                            fig = go.Figure()
                            
                            fig.add_trace(go.Scatter(
                                x=growth_df['Quarter_Label'],
                                y=growth_df['POW_YoY_Growth'],
                                mode='lines+markers',
                                name='POW',
                                line=dict(color='#1f77b4', width=2),
                                marker=dict(size=6),
                                hovertemplate='<b>%{x}</b><br>POW YoY Growth: %{y:.2f}%<extra></extra>'
                            ))
                            
                            fig.add_trace(go.Scatter(
                                x=growth_df['Quarter_Label'],
                                y=growth_df['NT2_YoY_Growth'],
                                mode='lines+markers',
                                name='NT2',
                                line=dict(color='#ff7f0e', width=2),
                                marker=dict(size=6),
                                hovertemplate='<b>%{x}</b><br>NT2 YoY Growth: %{y:.2f}%<extra></extra>'
                            ))
                            
                            fig.update_layout(
                                title='Contracted Volume YoY Growth Comparison (POW vs NT2)',
                                xaxis_title='Quarter',
                                yaxis_title='YoY Growth (%)',
                                height=500,
                                hovermode='x unified',
                                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
                            )
                            
                            # Add horizontal line at zero
                            fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Show data table
                            st.markdown("**📋 Contracted Volume Growth Data**")
                            display_df = growth_df[['Quarter_Label', 'POW_Contracted', 'POW_YoY_Growth', 'NT2_Contracted', 'NT2_YoY_Growth']].copy()
                            display_df.columns = ['Quarter', 'POW Volume (mn kWh)', 'POW YoY Growth (%)', 'NT2 Volume (mn kWh)', 'NT2 YoY Growth (%)']
                            display_df = display_df.round(2)
                            st.dataframe(display_df, use_container_width=True, hide_index=True)
                            
                            st.info("📌 **Note**: Monthly volumes are aggregated to quarterly totals before calculating YoY growth. POW volume includes Ca Mau (which has 0 contracted volume from 2019-2021).")
                            
                        else:
                            st.warning("No YoY growth data available (requires at least 4 quarters of data)")
                    else:
                        st.warning(f"POW Contracted or NT2 Contracted columns not found. Available columns: {list(df.columns)}")
            else:
                st.info("📊 Contracted volume data file not found. Expected: data/volume_pow_monthly.csv")
                
        except Exception as e:
            st.error(f"Error loading contracted volume data: {e}")
            import traceback
            st.error(traceback.format_exc())


def display_coal_strategy_from_csv():
    """Display coal strategy results from CSV file"""
    st.title("🔥 Coal Strategy Analysis")
    
    st.markdown("""
    ### Strategy Overview
    
    The coal strategy selects coal stocks (PPC, QTP, HND) based on sales volume growth.
    
    **Portfolio Types:**
    - **Diversified Portfolio**: Includes multiple stocks with positive growth
    - **Concentrated Portfolio**: Invests in stocks with highest sales volume growth
    """)
    
    # Load data
    df = load_coal_strategy_results()
    
    if df.empty:
        st.warning("No coal strategy data available")
        return
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📊 Performance Chart", "📋 Portfolio Details", "📈 Volume Growth"])
    
    with tab1:
        st.subheader("📈 Cumulative Returns Comparison")
        
        # Separate portfolios
        diversified_df = df[df['strategy_type'] == 'diversified'].copy()
        concentrated_df = df[df['strategy_type'] == 'concentrated'].copy()
        
        if not diversified_df.empty or not concentrated_df.empty:
            # Create performance chart
            fig = go.Figure()
            
            if not diversified_df.empty:
                fig.add_trace(go.Scatter(
                    x=diversified_df['period'],
                    y=diversified_df['cumulative_return'],
                    mode='lines+markers',
                    name='Diversified Portfolio',
                    line=dict(color='#9467bd', width=2),
                    marker=dict(size=6)
                ))
            
            if not concentrated_df.empty:
                fig.add_trace(go.Scatter(
                    x=concentrated_df['period'],
                    y=concentrated_df['cumulative_return'],
                    mode='lines+markers',
                    name='Concentrated Portfolio',
                    line=dict(color='#8c564b', width=2),
                    marker=dict(size=6)
                ))
            
            fig.update_layout(
                title='Coal Strategy Cumulative Returns',
                xaxis_title='Period',
                yaxis_title='Cumulative Return (%)',
                height=500,
                hovermode='x unified',
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary metrics
            col1, col2 = st.columns(2)
            with col1:
                if not diversified_df.empty:
                    final_return_div = diversified_df['cumulative_return'].iloc[-1]
                    st.metric("Diversified Portfolio", f"{final_return_div:.2f}%")
                else:
                    st.metric("Diversified Portfolio", "N/A")
            
            with col2:
                if not concentrated_df.empty:
                    final_return_con = concentrated_df['cumulative_return'].iloc[-1]
                    st.metric("Concentrated Portfolio", f"{final_return_con:.2f}%")
                else:
                    st.metric("Concentrated Portfolio", "N/A")
    
    with tab2:
        st.subheader("📋 Portfolio Composition Details")
        
        # Display concentrated portfolio
        if not concentrated_df.empty:
            st.markdown("#### 🎯 Concentrated Portfolio")
            display_df = concentrated_df[['period', 'selected_stocks', 'quarterly_return', 'cumulative_return', 'selection_based_on']].copy()
            display_df.columns = ['Period', 'Selected Stocks', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Selection Basis']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        if not diversified_df.empty:
            st.markdown("---")
            st.markdown("#### 🔀 Diversified Portfolio")
            display_df2 = diversified_df[['period', 'selected_stocks', 'quarterly_return', 'cumulative_return', 'selection_based_on']].copy()
            display_df2.columns = ['Period', 'Selected Stocks', 'Quarterly Return (%)', 'Cumulative Return (%)', 'Selection Basis']
            st.dataframe(display_df2, use_container_width=True, hide_index=True)
    
    with tab3:
        st.subheader("📈 Coal Sales Volume Growth Analysis")
        
        # Load coal volume data using coal_strategy methodology
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            volume_file = os.path.join(script_dir, 'data', 'coal_volume_quarterly.csv')
            
            if os.path.exists(volume_file):
                df = pd.read_csv(volume_file)
                
                # The file format: first column is period, then PPC, QTP, HND columns
                # Rename columns to standard format
                if len(df.columns) == 4:
                    df.columns = ['period', 'PPC', 'QTP', 'HND']
                else:
                    st.warning(f"Unexpected number of columns in coal data: {len(df.columns)}. Expected 4.")
                    st.write("Available columns:", list(df.columns))
                    df = None
                
                if df is not None:
                    # Remove any rows with empty periods
                    df = df.dropna(subset=['period'])
                    
                    # Convert numeric columns
                    for col in ['PPC', 'QTP', 'HND']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Clean period column and ensure proper format
                    df['period'] = df['period'].astype(str).str.strip()
                    
                    # Filter out any invalid periods (should match pattern like "1Q2018", "4Q2017", etc.)
                    df = df[df['period'].str.contains(r'\d+Q\d+', na=False)]
                    
                    # Convert period format from "1Q2018" to "2018Q1" to match other data
                    def convert_period_format(period_str):
                        try:
                            if 'Q' in period_str:
                                parts = period_str.split('Q')
                                if len(parts) == 2:
                                    quarter = parts[0]
                                    year = parts[1]
                                    return f"{year}Q{quarter}"
                            return period_str
                        except:
                            return period_str
                    
                    df['period'] = df['period'].apply(convert_period_format)
                    
                    # Sort by period
                    df = df.sort_values('period')
                    
                    # Calculate YoY growth for each coal stock (4 quarters back)
                    coal_companies = ['PPC', 'QTP', 'HND']
                    growth_data = []
                    
                    for company in coal_companies:
                        if company not in df.columns:
                            continue
                        
                        company_data = df[['period', company]].copy()
                        company_data = company_data.sort_values('period')
                        
                        # Calculate YoY growth (4 quarters back)
                        for i in range(4, len(company_data)):
                            current_period = company_data.iloc[i]['period']
                            current_value = company_data.iloc[i][company]
                            prev_value = company_data.iloc[i-4][company]
                            
                            if pd.notna(current_value) and pd.notna(prev_value) and prev_value != 0:
                                yoy_growth = (current_value - prev_value) / prev_value * 100
                                
                                growth_data.append({
                                    'period': current_period,
                                    'company': company,
                                    'volume': current_value,
                                    'yoy_growth': yoy_growth
                                })
                    
                    if growth_data:
                        growth_df = pd.DataFrame(growth_data)
                        
                        # Pivot the data for easier plotting
                        periods = sorted(growth_df['period'].unique())
                        
                        # Create comparison chart
                        fig = go.Figure()
                        
                        colors = {'PPC': '#8c564b', 'QTP': '#e377c2', 'HND': '#7f7f7f'}
                        
                        for company in coal_companies:
                            company_growth = growth_df[growth_df['company'] == company]
                            if not company_growth.empty:
                                fig.add_trace(go.Scatter(
                                    x=company_growth['period'],
                                    y=company_growth['yoy_growth'],
                                    mode='lines+markers',
                                    name=company,
                                    line=dict(color=colors.get(company, '#000000'), width=2),
                                    marker=dict(size=6),
                                    hovertemplate=f'<b>%{{x}}</b><br>{company} YoY Growth: %{{y:.2f}}%<extra></extra>'
                                ))
                        
                        fig.update_layout(
                            title='Coal Sales Volume YoY Growth Comparison (PPC, QTP, HND)',
                            xaxis_title='Quarter',
                            yaxis_title='YoY Growth (%)',
                            height=500,
                            hovermode='x unified',
                            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
                        )
                        
                        # Add horizontal line at zero
                        fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1)
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show data table
                        st.markdown("**📋 Coal Volume Growth Data**")
                        
                        # Create wide format table for display
                        display_data = []
                        for period in periods:
                            row = {'Quarter': period}
                            period_data = growth_df[growth_df['period'] == period]
                            
                            for company in coal_companies:
                                company_data = period_data[period_data['company'] == company]
                                if not company_data.empty:
                                    row[f'{company} Volume (mn kWh)'] = company_data.iloc[0]['volume']
                                    row[f'{company} YoY Growth (%)'] = company_data.iloc[0]['yoy_growth']
                                else:
                                    row[f'{company} Volume (mn kWh)'] = None
                                    row[f'{company} YoY Growth (%)'] = None
                            
                            display_data.append(row)
                        
                        display_df = pd.DataFrame(display_data)
                        display_df = display_df.round(2)
                        st.dataframe(display_df, use_container_width=True, hide_index=True)
                        
                        st.info("📌 **Note**: Data is already in quarterly format. YoY growth calculated by comparing to same quarter previous year (4 quarters back)")
                        
                    else:
                        st.warning("No YoY growth data available (requires at least 4 quarters of data)")
            else:
                st.info("📊 Coal volume data file not found. Expected: data/coal_volume_quarterly.csv")
                
        except Exception as e:
            st.error(f"Error loading coal volume data: {e}")
            import traceback
            st.error(traceback.format_exc())
