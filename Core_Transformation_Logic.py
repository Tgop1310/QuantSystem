"""
This code demonstrates the real transformation logic used in my multi-strategy trading system for the technical aspect ONLY.
It uses mostly uses Pandas and Numpy operations for technical analysis and signal generation.
This code also runs in airflow everyday

Note: Database connection details and proprietary calculations have been sanitized.
"""


import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def get_last_value(series: pd.Series) -> Union[float, str]:
    """Return the last non-NaN value of a series, or 'No Value' if empty."""
    if series is None:
        return "No Value"
    clean = series.dropna()
    return clean.iloc[-1] if not clean.empty else np.nan

def get_second_last_value(series: pd.Series) -> Union[float, str]:
    """Return the second last non-NaN value of a series, or NaN if unavailable."""
    if series is None:
        return "No Value"
    clean = series.dropna()
    return clean.iloc[-2] if len(clean) > 1 else np.nan

def load_stock_list(file_path: str, sheet_name: str = 'Sheet2') -> List[str]:
    """Load list of stock symbols from Excel, clean, and filter to alpha strings."""
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df['Symbol'] = df['Symbol'].astype(str).str.strip().str.replace('.', '-', regex=False)
    stock_list = df['Symbol'].dropna().astype(str).to_list()
    return [s for s in stock_list if s.isalpha()]

def connect_postgres(username: str, password: str, host: str, port: str, database: str):
    """Return a SQLAlchemy engine to connect to PostgreSQL."""
    return create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

def load_stock_data(engine, table_name: str) -> pd.DataFrame:
    """Load stock data from PostgreSQL table into a DataFrame."""
    sql = f'SELECT * FROM {table_name}'
    return pd.read_sql(sql, engine)

def calculate_indicators(df: pd.DataFrame, df_wk: pd.DataFrame, stock_list: List[str]) -> pd.DataFrame:
    """Calculate technical indicators and trading signals for a list of stocks."""
    df_signals = pd.DataFrame(index=stock_list)
    
    # Initialize all signal columns
    signal_columns = [
        "Last_Above_Upper_Band", "Last_below_Lower_Band", "RSI", "Signal", "MACD",
        "Signal_2", "20_SMA", "50_SMA", "100_SMA", "200_SMA", "SMA_Crossover_20_50_Signal",
        "SMA_Crossover_100_200_Signal", "EMA_6_10_Crossover_Signal", "Fib_23_6", "Fib_38_2",
        "Fib_50_0", "Fib_61_8", "Fib_78_6", "Daily_Delta_Volume", "Weekly_Delta_Volume"
    ]
    
    for col in signal_columns:
        df_signals[col] = "No Signal"  

    df_mass = {}  # For mass data storage
    missing_stocks = []

    for stock in stock_list:
        df_filter = df[df["Ticker"] == stock].copy()
        df_filter_wk = df_wk[df_wk["Ticker"] == stock].copy()

        if df_filter.empty:
            missing_stocks.append(stock)
            continue

        # Technical Indicators 
        close = df_filter['Close'].reset_index(drop=True)
        df_mass[f'{stock}_close'] = close
        sma_20 = close.rolling(window=20).mean()
        df_mass[f'{stock}_SMA_20'] = sma_20
        sd = close.rolling(window=20).std()
        df_mass[f'{stock}_SD'] = sd
        upper_band = sma_20 + 2 * sd
        lower_band = sma_20 - 2 * sd
        df_mass[f'{stock}_Upper_Band'] = upper_band
        df_mass[f'{stock}_Lower_Band'] = lower_band

        # MACD
        ema_short = close.ewm(span=12, adjust=False).mean()
        ema_long = close.ewm(span=26, adjust=False).mean()
        macd = ema_short - ema_long
        signal_line = macd.ewm(span=9, adjust=False).mean()
        df_mass[f'{stock}_MACD'] = macd
        df_mass[f'{stock}_Signal_Line'] = signal_line

        # Bollinger Band signals
        if close.iloc[-1] > upper_band.iloc[-1]:
            df_signals.loc[stock, "Last_Above_Upper_Band"] = "True"
        elif close.iloc[-1] < lower_band.iloc[-1]:
            df_signals.loc[stock, "Last_below_Lower_Band"] = "True"

        # RSI Calculation
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(14, min_periods=1).mean()
        avg_loss = loss.rolling(14, min_periods=1).mean()
        rsi = 100 - (100 / (1 + (avg_gain / avg_loss)))
        df_signals.loc[stock, "RSI"] = rsi.iloc[-1]

        # MACD signals
        if macd.iloc[-1] > signal_line.iloc[-1]:
            df_signals.loc[stock, "MACD"] = "BUY"
        elif macd.iloc[-1] < signal_line.iloc[-1]:
            df_signals.loc[stock, "MACD"] = "SELL"

        # SMA / EMA / Crossovers
        sma_50 = close.rolling(50, min_periods=1).mean()
        sma_100 = close.rolling(100, min_periods=1).mean()
        sma_200 = close.rolling(200, min_periods=1).mean()
        ema_6 = close.ewm(span=6, adjust=False).mean()
        ema_10 = close.ewm(span=10, adjust=False).mean()

        df_signals.loc[stock, "20_SMA"] = get_last_value(sma_20)
        df_signals.loc[stock, "50_SMA"] = get_last_value(sma_50)
        df_signals.loc[stock, "100_SMA"] = get_last_value(sma_100)
        df_signals.loc[stock, "200_SMA"] = get_last_value(sma_200)

        # SMA crossovers
        cross_up_20_50 = (sma_20 > sma_50) & (sma_20.shift(1) <= sma_50.shift(1))
        cross_down_20_50 = (sma_20 < sma_50) & (sma_20.shift(1) >= sma_50.shift(1))
        cross_up_100_200 = (sma_100 > sma_200) & (sma_100.shift(1) <= sma_200.shift(1))
        cross_down_100_200 = (sma_100 < sma_200) & (sma_100.shift(1) >= sma_200.shift(1))
        ema_cross_up_6_10 = (ema_6 > ema_10) & (ema_6.shift(1) <= ema_10.shift(1))
        ema_cross_down_6_10 = (ema_6 < ema_10) & (ema_6.shift(1) >= ema_10.shift(1))

        df_signals.loc[stock, "SMA_Crossover_20_50_Signal"] = "BUY" if cross_up_20_50.iloc[-1] else "SELL" if cross_down_20_50.iloc[-1] else "No Signal"
        df_signals.loc[stock, "SMA_Crossover_100_200_Signal"] = "BUY" if cross_up_100_200.iloc[-1] else "SELL" if cross_down_100_200.iloc[-1] else "No Signal"
        df_signals.loc[stock, "EMA_6_10_Crossover_Signal"] = "BUY" if ema_cross_up_6_10.iloc[-1] else "SELL" if ema_cross_down_6_10.iloc[-1] else "No Signal"

        # Above SMA signals
        df_signals.loc[stock, "Above_20_SMA"] = "BUY" if close.iloc[-1] > sma_20.iloc[-1] else "SELL"
        df_signals.loc[stock, "Above_50_SMA"] = "BUY" if close.iloc[-1] > sma_50.iloc[-1] else "SELL"
        df_signals.loc[stock, "Above_100_SMA"] = "BUY" if close.iloc[-1] > sma_100.iloc[-1] else "SELL"
        df_signals.loc[stock, "Above_200_SMA"] = "BUY" if close.iloc[-1] > sma_200.iloc[-1] else "SELL"

        # Fib retracement signals
        weekly_high = get_last_value(df_filter_wk["High"])
        weekly_low = get_second_last_value(df_filter_wk["Low"])
        fib_levels = {
            "Fib_23_6": weekly_high - (weekly_high - weekly_low) * 0.236,
            "Fib_38_2": weekly_high - (weekly_high - weekly_low) * 0.382,
            "Fib_50_0": weekly_high - (weekly_high - weekly_low) * 0.5,
            "Fib_61_8": weekly_high - (weekly_high - weekly_low) * 0.618,
            "Fib_78_6": weekly_high - (weekly_high - weekly_low) * 0.786,
        }
        for level, value in fib_levels.items():
            if close.iloc[-1] > value:
                df_signals.loc[stock, level] = "BUY"
            else:
                df_signals.loc[stock, level] = "SELL"

        # Daily & weekly delta volume signals
        daily_volume = 'Higher Volume' if get_last_value(df_filter["Volume"]) >= 1.2 * get_second_last_value(df_filter['Volume']) else 'Lower Volume'
        weekly_volume = 'Higher Volume' if get_last_value(df_filter_wk["Volume"]) >= 1.2 * get_second_last_value(df_filter_wk['Volume']) else 'Lower Volume'

        df_signals.loc[stock, "Daily_Delta_Volume"] = "Buy" if close.iloc[-1] >= 1.02 * get_second_last_value(close) and daily_volume == 'Higher Volume' else "Sell" if close.iloc[-1] <= 0.98 * get_second_last_value(close) and daily_volume == 'Lower Volume' else "No Signal"
        df_signals.loc[stock, "Weekly_Delta_Volume"] = "Buy" if get_last_value(df_filter_wk['High']) >= 1.02 * get_second_last_value(df_filter_wk["Close"]) and weekly_volume == 'Higher Volume' else "Sell" if get_last_value(df_filter_wk['Low']) <= 0.98 * get_second_last_value(df_filter_wk["Close"]) and weekly_volume == 'Lower Volume' else "No Signal"

    df_mass = pd.DataFrame(df_mass)
    df_signals = df_signals.applymap(lambda x: x.item() if isinstance(x, np.generic) else x)

    return df_mass, df_signals, missing_stocks

def save_output(df_mass: pd.DataFrame, df_signals: pd.DataFrame,
                output_excel: str, output_csv: str, engine, schema: str = 'stock_db'):
    """Save the outputs to CSV, Excel, and PostgreSQL."""
    df_mass.to_csv(output_csv, index=True)
    df_signals.to_excel(output_excel, index=True)
    df_signals.to_sql('Stock_Signals', engine, if_exists='replace', index=True, schema=schema)

def daily_signals_update():
    """Main function to update daily stock signals."""
    # Load stock list
    stock_list = load_stock_list('input/Stock_List.xlsx')

    # Connect to PostgreSQL
    engine = connect_postgres(username='YOUR_USERNAME',
                              password='YOUR_PASSWORD',
                              host='localhost',
                              port='5432',
                              database='YOUR_DB')

    # Load stock data
    df_daily = load_stock_data(engine, 'stock_db."Mass_stock_data"')
    df_weekly = load_stock_data(engine, 'stock_db."Mass_weekly_stock_data"')

    # Calculate indicators & signals
    df_mass, df_signals, missing_stocks = calculate_indicators(df_daily, df_weekly, stock_list)

    # Save outputs
    save_output(df_mass, df_signals,
                output_excel='output/db_DAG_signal_testing.xlsx',
                output_csv='output/db_DAG_Mass_Data_testing.csv',
                engine=engine)

    print(f"Missing stocks: {missing_stocks}")
