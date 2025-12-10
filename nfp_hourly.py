"""
Script: nfp_hourly.py
Description: Fetches Non-Farm Payrolls data (monthly change in total nonfarm employment) 
             and aligns it with hourly stock data, forward-filling from each NFP release.
"""

import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime
from zoneinfo import ZoneInfo

# FRED series for total nonfarm payroll employment (in thousands of persons, seasonally adjusted)
PAYROLLS_SERIES = "PAYEMS"  # All Employees: Total Nonfarm (in thousands)

def fetch_payrolls_data(start_date="2022-01-01"):
    """
    Fetch total nonfarm payrolls level from FRED starting at start_date.
    Returns a pandas Series of monthly payroll levels (thousands of employees).
    """
    payrolls = pdr.DataReader(PAYROLLS_SERIES, "fred", start_date)
    return payrolls[PAYROLLS_SERIES] if isinstance(payrolls, pd.DataFrame) else payrolls

def compute_monthly_change(series):
    """
    Compute monthly change in the series (difference between current and previous month).
    Returns a pandas Series of the changes.
    """
    series = series.sort_index()
    change = series.diff().rename("NFP_Change")  # difference (in same units, thousands)
    return change.dropna()

def get_nfp_release_schedule(nfp_series):
    """
    Approximate release datetimes for each nonfarm payroll observation.
    Releases occur at 8:30 AM ET, usually on the first Friday of the month for the prior month's data.
    Returns list of release datetimes in UTC.
    """
    releases = []
    ny_tz = ZoneInfo("America/New_York")
    for date in nfp_series.index:
        # date is typically the month (e.g., 2023-10-01 for October data, released in early Nov)
        year = date.year
        month = date.month
        # The report for month (M) is released in early month (M+1) except for year transitions
        release_month = month + 1
        release_year = year
        if release_month == 13:
            release_month = 1
            release_year += 1
        # Default to 1st day of release_month, then find first Friday at 8:30
        release_dt = datetime(release_year, release_month, 1, 8, 30, tzinfo=ny_tz)
        # Advance to first Friday (weekday 4) of that month
        while release_dt.weekday() != 4:  # Monday=0,...Friday=4
            release_dt += pd.Timedelta(days=1)
        release_dt_utc = release_dt.astimezone(ZoneInfo("UTC"))
        releases.append(release_dt_utc)
    return releases

def align_nfp_to_hours(nfp_changes, hourly_index):
    """
    Align monthly NFP change values to the hourly trading index.
    Forward-fill each value from its 8:30 AM release time to just before the next release.
    Returns DataFrame with NonFarm_Payrolls_Change column.
    """
    # Ensure hourly_index is a timezone-aware DatetimeIndex in UTC
    hourly_index = pd.to_datetime(hourly_index, errors="coerce")
    hourly_index = pd.DatetimeIndex(hourly_index)
    hourly_index = hourly_index.dropna()
    if hourly_index.tz is None:
        hourly_index = hourly_index.tz_localize("UTC")
    else:
        hourly_index = hourly_index.tz_convert("UTC")
    
    release_times = get_nfp_release_schedule(nfp_changes)
    values = nfp_changes.values
    df = pd.DataFrame(index=hourly_index, columns=["NonFarm_Payrolls_Change"], dtype=float)
    for i, rel_time in enumerate(release_times):
        val = values[i]
        if i < len(release_times) - 1:
            next_time = release_times[i+1]
        else:
            next_time = hourly_index[-1] + pd.Timedelta(hours=1)
        # Both rel_time and next_time are UTC-aware; compare directly with hourly_index
        mask = (df.index >= rel_time) & (df.index < next_time)
        df.loc[mask, "NonFarm_Payrolls_Change"] = val
    # Forward fill any initial gaps
    df.fillna(method='ffill', inplace=True)
    return df

if __name__ == "__main__":
    # Fetch payroll data from FRED
    payrolls = fetch_payrolls_data(start_date="2022-01-01")
    # Compute monthly change (NFP change in thousands)
    nfp_change = compute_monthly_change(payrolls)
    # Load trading hour timeline from a sample stock (AAPL)
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
    # Align NFP changes to hourly timeline
    nfp_hourly_df = align_nfp_to_hours(nfp_change, hourly_index)
    # Save to CSV
    nfp_hourly_df.reset_index(inplace=True)
    nfp_hourly_df.rename(columns={"index": "Datetime"}, inplace=True)
    nfp_hourly_df.to_csv("NFP_hourly.csv", index=False)
    print("Saved Non-Farm Payrolls changes to NFP_hourly.csv")
