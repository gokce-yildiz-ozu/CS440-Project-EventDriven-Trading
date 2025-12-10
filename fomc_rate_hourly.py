"""
Script: fomc_rate_hourly.py
Description: Fetches the Fed Funds target rate changes and aligns them to hourly stock data, 
             forward-filling from each FOMC announcement (2:00 PM ET) onward.
"""

import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime, time
from zoneinfo import ZoneInfo

# FRED series for Federal Funds Target Rate (Upper Bound)
FED_RATE_SERIES = "DFEDTARU"  # Federal Funds Target Range: Upper Limit (%)

def fetch_fomc_rate_series(start_date="2023-01-01"):
    """
    Fetch the Fed Funds target rate (upper bound) daily series from FRED starting from start_date.
    Returns a pandas Series of daily rates (%).
    """
    rate_df = pdr.DataReader(FED_RATE_SERIES, "fred", start_date)
    # Series indexed by date
    rate_series = rate_df[FED_RATE_SERIES] if isinstance(rate_df, pd.DataFrame) else rate_df
    return rate_series.dropna()

def get_fomc_decision_times(rate_series):
    """
    Identify FOMC decision datetimes (UTC) and corresponding rates.
    This finds dates where the target rate changed or where a meeting occurred (for completeness, include unchanged if needed).
    Returns a list of (decision_datetime_UTC, new_rate).
    """
    # Find all dates where rate changes (or all Fed meeting dates).
    rate_changes = []
    prev_rate = None
    ny_tz = ZoneInfo("America/New_York")
    for date, rate in rate_series.items():
        if prev_rate is None or rate != prev_rate:
            # Decision on this date (rate changed or first observed)
            decision_time_local = datetime.combine(date, time(14, 0), tzinfo=ny_tz)  # 2:00 PM ET on that date
            decision_time_utc = decision_time_local.astimezone(ZoneInfo("UTC"))
            rate_changes.append((decision_time_utc, rate))
            prev_rate = rate
    return rate_changes

def align_rate_to_hours(rate_changes, hourly_index):
    """
    Align Fed rate values to hourly timeline.
    Forward-fill from each decision time (2:00 PM ET on decision day) onward until next decision.
    Returns DataFrame with a Fed_Funds_Rate column (%).
    """
    # Ensure hourly_index is a timezone-aware DatetimeIndex in UTC
    hourly_index = pd.to_datetime(hourly_index, errors="coerce")
    hourly_index = pd.DatetimeIndex(hourly_index)
    hourly_index = hourly_index.dropna()
    if hourly_index.tz is None:
        hourly_index = hourly_index.tz_localize("UTC")
    else:
        hourly_index = hourly_index.tz_convert("UTC")
    
    df = pd.DataFrame(index=hourly_index, columns=["Fed_Funds_Rate"], dtype=float)
    for i, (dec_time, new_rate) in enumerate(rate_changes):
        if i < len(rate_changes) - 1:
            next_dec_time = rate_changes[i+1][0]
        else:
            next_dec_time = hourly_index[-1] + pd.Timedelta(hours=1)
        # Both dec_time and next_dec_time are UTC-aware; compare directly with hourly_index
        mask = (df.index >= dec_time) & (df.index < next_dec_time)
        df.loc[mask, "Fed_Funds_Rate"] = new_rate
    # Back-fill the initial period before the first recorded decision with the first rate
    df.fillna(method='bfill', inplace=True)
    return df

if __name__ == "__main__":
    # Fetch Fed target rate series (daily)
    rate_series = fetch_fomc_rate_series(start_date="2023-01-01")
    # Determine FOMC decision times and new rates
    decisions = get_fomc_decision_times(rate_series)
    # Load trading hour index from stock data
    stock_df = pd.read_csv(
        "data/raw/market/AAPL_1h.csv",
        skiprows=3,
        header=None,
        names=["Datetime", "Close", "High", "Low", "Open", "Volume"],
        dtype={"Close": float, "High": float, "Low": float, "Open": float, "Volume": float},
    )
    # Parse datetimes, coerce invalid strings to NaT, then drop those rows
    stock_df["Datetime"] = pd.to_datetime(stock_df["Datetime"], utc=True, errors="coerce")
    stock_df = stock_df.dropna(subset=["Datetime"]).reset_index(drop=True)
    hourly_index = stock_df["Datetime"]
    # Align rate changes to hourly timeline
    rate_hourly_df = align_rate_to_hours(decisions, hourly_index)
    # Save to CSV
    rate_hourly_df.reset_index(inplace=True)
    rate_hourly_df.rename(columns={"index": "Datetime"}, inplace=True)
    rate_hourly_df.to_csv("FOMC_rate_hourly.csv", index=False)
    print("Saved FOMC rate decisions data to FOMC_rate_hourly.csv")
