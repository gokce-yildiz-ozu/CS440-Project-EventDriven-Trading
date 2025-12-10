"""
Script: ppi_inflation_hourly.py
Description: Fetches PPI data and computes YoY inflation, aligning it with hourly stock data 
             by forward-filling from each PPI release (8:30 AM ET) onward.
"""

import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime
from zoneinfo import ZoneInfo

# FRED series for PPI Final Demand index (seasonally adjusted)
PPI_SERIES = "PPIFIS"  # Producer Price Index by Commodity: Final Demand (Index)

def fetch_ppi_index(start_date="2022-01-01"):
    """
    Fetch PPI final demand index from FRED starting at start_date.
    Returns a pandas Series of monthly PPI index values.
    """
    ppi_df = pdr.DataReader(PPI_SERIES, "fred", start_date)
    ppi = ppi_df[PPI_SERIES] if isinstance(ppi_df, pd.DataFrame) else ppi_df
    return ppi.dropna()

def compute_yoy_inflation(index_series):
    """
    Compute year-over-year inflation rate (%) from an index series.
    """
    index_series = index_series.sort_index()
    yoy = (index_series.pct_change(periods=12) * 100).rename("PPI_YoY")
    return yoy.dropna()

def get_ppi_release_schedule(ppi_series):
    """
    Determine approximate PPI release datetimes (8:30 AM ET) for each monthly observation.
    PPI is typically released in the second week of the following month (often one day after CPI).
    Returns list of release datetimes in UTC.
    """
    releases = []
    ny_tz = ZoneInfo("America/New_York")
    for date in ppi_series.index:
        year = date.year
        month = date.month
        # Next month for release
        release_month = month + 1
        release_year = year
        if release_month == 13:
            release_month = 1
            release_year += 1
        # Tentatively set release on 14th of next month at 8:30 ET (PPI often around then)
        release_day = 14
        release_dt_local = datetime(release_year, release_month, release_day, 8, 30, tzinfo=ny_tz)
        # If weekend, adjust to next Monday
        while release_dt_local.weekday() >= 5:
            release_day += 1
            release_dt_local = datetime(release_year, release_month, release_day, 8, 30, tzinfo=ny_tz)
        release_dt_utc = release_dt_local.astimezone(ZoneInfo("UTC"))
        releases.append(release_dt_utc)
    return releases

def align_ppi_to_hours(ppi_yoy, hourly_index):
    """
    Align PPI YoY values to the hourly trading index by forward-filling from each release time.
    Returns DataFrame with PPI_YoY column.
    """
    # Ensure hourly_index is a timezone-aware DatetimeIndex in UTC
    hourly_index = pd.to_datetime(hourly_index, errors="coerce")
    hourly_index = pd.DatetimeIndex(hourly_index)
    hourly_index = hourly_index.dropna()
    if hourly_index.tz is None:
        hourly_index = hourly_index.tz_localize("UTC")
    else:
        hourly_index = hourly_index.tz_convert("UTC")
    
    release_times = get_ppi_release_schedule(ppi_yoy)
    values = ppi_yoy.values
    df = pd.DataFrame(index=hourly_index, columns=["PPI_YoY"], dtype=float)
    for i, rel_time in enumerate(release_times):
        val = values[i]
        next_time = release_times[i+1] if i < len(release_times)-1 else hourly_index[-1] + pd.Timedelta(hours=1)
        # Both rel_time and next_time are UTC-aware; compare directly with hourly_index
        mask = (df.index >= rel_time) & (df.index < next_time)
        df.loc[mask, "PPI_YoY"] = val
    df.fillna(method='ffill', inplace=True)
    return df

if __name__ == "__main__":
    # Fetch PPI index data from FRED
    ppi_index = fetch_ppi_index(start_date="2022-01-01")
    # Compute YoY inflation rate (%)
    ppi_yoy = compute_yoy_inflation(ppi_index)
    # Load stock hourly timeline
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
    # Align PPI YoY to hourly data
    ppi_hourly_df = align_ppi_to_hours(ppi_yoy, hourly_index)
    # Save to CSV
    ppi_hourly_df.reset_index(inplace=True)
    ppi_hourly_df.rename(columns={"index": "Datetime"}, inplace=True)
    ppi_hourly_df.to_csv("PPI_hourly.csv", index=False)
    print("Saved PPI YoY data to PPI_hourly.csv")
