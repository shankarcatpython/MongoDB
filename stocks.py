import yfinance as yf
import pandas as pd
import plotly.graph_objs as go
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Function to pull last 3 years of daily transactional data for any ticker
def fetch_transactional_data(ticker_symbol):
    # Calculate the date range for the last 3 years
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=3)
    
    # Download the historical market data from Yahoo Finance
    ticker_data = yf.download(ticker_symbol, start=start_date, end=end_date, interval='1d')
    
    # Reset index to make Date a column and convert it to JSON format
    ticker_data.reset_index(inplace=True)
    
    # Save to a JSON file
    ticker_data.to_json(f'{ticker_symbol}_transactional_data.json', orient='records', date_format='iso')
    
    print(f"Transactional data for last 3 years saved to '{ticker_symbol}_transactional_data.json'.")
    return ticker_data

# Function to pull quarterly financial data for specific metrics (last 12 quarters)
def fetch_quarterly_financial_data(ticker_symbol):
    # Download ticker object
    ticker = yf.Ticker(ticker_symbol)
    
    # Fetch the financials (quarterly data)
    financials = ticker.quarterly_financials.T  # Transpose to make rows as quarters
    
    # Fetch cash flow (quarterly data)
    cashflow = ticker.quarterly_cashflow.T
    
    # Fetch balance sheet for debt (quarterly data)
    balance_sheet = ticker.quarterly_balance_sheet.T
    
    # Fetch additional stats like profit margin and PE ratio from yahoo finance info
    info = ticker.info
    
    # Check available columns in cashflow
    print("Available cash flow columns:", cashflow.columns)
    
    # Use a safe fallback in case 'Total Cash From Operating Activities' is missing
    operating_cash_flow_column = 'Total Cash From Operating Activities' if 'Total Cash From Operating Activities' in cashflow.columns else cashflow.columns[0]  # Default to first column if not found
    
    # Prepare a DataFrame for quarterly financial summary
    quarterly_data = pd.DataFrame({
        'NetRevenue': financials.get('Total Revenue', None),  # Use .get to handle missing columns
        'OperatingCashFlow': cashflow.get(operating_cash_flow_column, None),  # Handle missing cash flow
        'NetIncome': financials.get('Net Income', None),
        'TotalDebt': balance_sheet.get('Long Term Debt', None),
        'ProfitMargin': info.get('profitMargins', None),  # Fetch from ticker info
        'PERatio': info.get('trailingPE', None)  # Fetch PE ratio from ticker info
    })
    
    # Save to a JSON file
    quarterly_data.to_json(f'{ticker_symbol}_quarterly_financial_data.json', orient='index', date_format='iso')
    
    print(f"Quarterly financial data for last 12 quarters saved to '{ticker_symbol}_quarterly_financial_data.json'.")
    return quarterly_data

# Function to plot the daily closing prices with moving averages (last 3 years) using Plotly
def plot_transactional_data(ticker_data, ticker_symbol):
    # Add moving averages to the data
    ticker_data['50_MA'] = ticker_data['Close'].rolling(window=50).mean()
    ticker_data['100_MA'] = ticker_data['Close'].rolling(window=100).mean()
    ticker_data['200_MA'] = ticker_data['Close'].rolling(window=200).mean()
    
    # Create interactive plot with Plotly
    fig = go.Figure()

    # Add closing price line
    fig.add_trace(go.Scatter(x=ticker_data['Date'], y=ticker_data['Close'], mode='lines', name='Closing Price'))
    
    # Add moving averages
    fig.add_trace(go.Scatter(x=ticker_data['Date'], y=ticker_data['50_MA'], mode='lines', name='50-Day MA'))
    fig.add_trace(go.Scatter(x=ticker_data['Date'], y=ticker_data['100_MA'], mode='lines', name='100-Day MA'))
    fig.add_trace(go.Scatter(x=ticker_data['Date'], y=ticker_data['200_MA'], mode='lines', name='200-Day MA'))

    # Customize layout
    fig.update_layout(
        title=f'{ticker_symbol} Closing Prices & Moving Averages (Last 3 Years)',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        hovermode='x unified'
    )

    # Show the interactive chart
    fig.show()

# Function to format numbers in billions for chart display
def format_billions(x):
    return f'{x * 1e-9:.1f}B'  # Format numbers as billions (B)

# Function to plot quarterly financial data as bar charts (last 12 quarters) using Plotly
def plot_quarterly_financial_data(quarterly_data, ticker_symbol):
    # Convert the index to PeriodIndex to represent quarters as "Year-Q1", "Year-Q2", etc.
    quarterly_data.index = pd.PeriodIndex(quarterly_data.index, freq='Q').strftime('%Y-Q%q')
    
    # Sort the data from oldest to newest
    quarterly_data = quarterly_data.sort_index(ascending=True)
    
    # Remove any rows with missing data
    quarterly_data = quarterly_data.dropna()

    # Convert values to billions
    quarterly_data['NetRevenue'] = quarterly_data['NetRevenue'] / 1e9
    quarterly_data['OperatingCashFlow'] = quarterly_data['OperatingCashFlow'] / 1e9
    quarterly_data['NetIncome'] = quarterly_data['NetIncome'] / 1e9
    quarterly_data['TotalDebt'] = quarterly_data['TotalDebt'] / 1e9

    # Create interactive bar chart with Plotly
    fig = go.Figure()

    # Add bar traces for each financial metric
    fig.add_trace(go.Bar(x=quarterly_data.index, y=quarterly_data['NetRevenue'], name='Net Revenue'))
    fig.add_trace(go.Bar(x=quarterly_data.index, y=quarterly_data['OperatingCashFlow'], name='Operating Cash Flow'))
    fig.add_trace(go.Bar(x=quarterly_data.index, y=quarterly_data['NetIncome'], name='Net Income'))
    fig.add_trace(go.Bar(x=quarterly_data.index, y=quarterly_data['TotalDebt'], name='Total Debt'))

    # Customize layout
    fig.update_layout(
        title=f'{ticker_symbol} Quarterly Financial Data (Last 12 Quarters)',
        xaxis_title='Quarter',
        yaxis_title='Amount (Billions USD)',  # Update the y-axis label to billions
        hovermode='x',
        barmode='group'
    )

    # Show the interactive chart
    fig.show()

# Function to pull annual financial data for specific metrics (last 3 years)
def fetch_annual_financial_data(ticker_symbol):
    # Download ticker object
    ticker = yf.Ticker(ticker_symbol)
    
    # Fetch the financials (annual data)
    financials = ticker.financials.T  # Transpose to make rows as years
    
    # Fetch cash flow (annual data)
    cashflow = ticker.cashflow.T
    
    # Fetch balance sheet for debt (annual data)
    balance_sheet = ticker.balance_sheet.T
    
    # Fetch additional stats like profit margin and PE ratio from yahoo finance info
    info = ticker.info
    
    # Use a safe fallback in case 'Total Cash From Operating Activities' is missing
    operating_cash_flow_column = 'Total Cash From Operating Activities' if 'Total Cash From Operating Activities' in cashflow.columns else cashflow.columns[0]  # Default to first column if not found
    
    # Prepare a DataFrame for annual financial summary
    annual_data = pd.DataFrame({
        'NetRevenue': financials.get('Total Revenue', None),  # Use .get to handle missing columns
        'OperatingCashFlow': cashflow.get(operating_cash_flow_column, None),  # Handle missing cash flow
        'NetIncome': financials.get('Net Income', None),
        'TotalDebt': balance_sheet.get('Long Term Debt', None)
    })
    
    # Save to a JSON file
    annual_data.to_json(f'{ticker_symbol}_annual_financial_data.json', orient='index', date_format='iso')
    
    print(f"Annual financial data for last 3 years saved to '{ticker_symbol}_annual_financial_data.json'.")
    return annual_data

# Function to plot annual financial data as bar charts (last 3 years) using Plotly
def plot_annual_financial_data(annual_data, ticker_symbol):
    annual_data.index = pd.to_datetime(annual_data.index).strftime('%Y')  # Format date as year only
    
    # Sort the data from oldest to newest
    annual_data = annual_data.sort_index(ascending=True)

    # Remove any rows with missing data
    annual_data = annual_data.dropna()

    # Convert values to billions
    annual_data['NetRevenue'] = annual_data['NetRevenue'] / 1e9
    annual_data['OperatingCashFlow'] = annual_data['OperatingCashFlow'] / 1e9
    annual_data['NetIncome'] = annual_data['NetIncome'] / 1e9
    annual_data['TotalDebt'] = annual_data['TotalDebt'] / 1e9

    # Create interactive bar chart with Plotly
    fig = go.Figure()

    # Add bar traces for each financial metric
    fig.add_trace(go.Bar(x=annual_data.index, y=annual_data['NetRevenue'], name='Net Revenue'))
    fig.add_trace(go.Bar(x=annual_data.index, y=annual_data['OperatingCashFlow'], name='Operating Cash Flow'))
    fig.add_trace(go.Bar(x=annual_data.index, y=annual_data['NetIncome'], name='Net Income'))
    fig.add_trace(go.Bar(x=annual_data.index, y=annual_data['TotalDebt'], name='Total Debt'))

    # Customize layout
    fig.update_layout(
        title=f'{ticker_symbol} Annual Financial Data (Last 3 Years)',
        xaxis_title='Year',
        yaxis_title='Amount (Billions USD)',  # Update the y-axis label to billions
        hovermode='x',
        barmode='group'
    )

    # Show the interactive chart
    fig.show()

# Main function to fetch and plot data
def main(ticker_symbol):
    # Fetch transactional and financial data
    transactional_data = fetch_transactional_data(ticker_symbol)
    quarterly_financial_data = fetch_quarterly_financial_data(ticker_symbol)
    # Fetch annual financial data
    annual_financial_data = fetch_annual_financial_data(ticker_symbol)

    # Plot transactional data and financial data
    plot_transactional_data(transactional_data, ticker_symbol)
    plot_quarterly_financial_data(quarterly_financial_data, ticker_symbol)
    # Plot annual financial data
    plot_annual_financial_data(annual_financial_data, ticker_symbol)


# Example usage with Meta's ticker 'META'
ticker = 'ADBE'
main(ticker)
