"""
Script: cpi_inflation_hourly.py
Description: Fetches CPI data (YoY and MoM inflation rates) and aligns it with 
             hourly intraday stock timestamps, forward-filling values from each 
             CPI release time (8:30 AM ET) onward to support merging with stock data.
"""

import pandas as pd
from pandas_datareader import data as pdr
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+ tzinfo

# FRED series identifiers for CPI (seasonally adjusted CPI index for all items)
CPI_SERIES = "CPIAUCSL"  # Consumer Price Index for All Urban Consumers: All Items

def fetch_cpi_index(start_date="2022-01-01", api_key="4fc8749afd7fbb8dd567d8a8ba10ff10"):
    """
    Fetch CPI index data from FRED starting from start_date. 
    Returns a pandas Series of monthly CPI values indexed by date.
    """
    # If using an API key with fredapi (optional):
    from fredapi import Fred
    fred = Fred(api_key=api_key)
    cpi_series = fred.get_series(CPI_SERIES, observation_start=start_date)
    return cpi_series
    
    # Using pandas_datareader (no API key required for FRED if not provided)
    cpi = pdr.DataReader(CPI_SERIES, "fred", start_date)
    return cpi[CPI_SERIES] if isinstance(cpi, pd.DataFrame) else cpi

def compute_inflation_rates(cpi_series):
    """
    Compute YoY and MoM inflation rates (in %) from a Series of CPI index values.
    Returns two pandas Series (YoY%, MoM%) indexed by the same dates (monthly).
    """
    # Ensure data is sorted by date
    cpi_series = cpi_series.sort_index()
    # Year-over-year percentage change: ((CPI_t / CPI_{t-12}) - 1) * 100
    cpi_yoy = (cpi_series.pct_change(periods=12) * 100).rename("CPI_YoY")
    # Month-over-month percentage change: ((CPI_t / CPI_{t-1}) - 1) * 100
    cpi_mom = (cpi_series.pct_change(periods=1) * 100).rename("CPI_MoM")
    # Drop initial NaN values (for first year and first month)
    return cpi_yoy.dropna(), cpi_mom.dropna()

def get_cpi_release_schedule(cpi_series):
    """
    Determine approximate CPI release datetimes (Eastern Time at 8:30 AM) for each CPI observation.
    We assume each monthly CPI (for month M) is released around mid-next-month at 8:30 ET.
    Returns a list of (release_datetime_UTC, CPI_yoy_value, CPI_mom_value).
    """
    releases = []
    ny_tz = ZoneInfo("America/New_York")
    # CPI series index is typically the period end (e.g. '2023-10-01' for October 2023).
    for date, value in cpi_series.items():
        # We'll use the next month's release date (which is known schedule: usually between 10th-15th).
        # If the CPI index date is, say, 2023-10-01 (Oct 2023 data), release would be in Nov 2023.
        year = date.year
        month = date.month
        # Next month for release
        release_month = month + 1
        release_year = year
        if release_month == 13:
            release_month = 1
            release_year += 1
        # Default release day as 10th of next month (will adjust if weekend)
        release_day = 10
        release_dt_local = datetime(release_year, release_month, release_day, 8, 30, tzinfo=ny_tz)
        # If release falls on a weekend, push to next Monday
        while release_dt_local.weekday() >= 5:  # 5=Saturday, 6=Sunday
            release_day += 1
            release_dt_local = datetime(release_year, release_month, release_day, 8, 30, tzinfo=ny_tz)
        # Adjust release date for known schedule variations if needed (this is a simple heuristic).
        release_dt_utc = release_dt_local.astimezone(ZoneInfo("UTC"))
        releases.append(release_dt_utc)
    return releases

def align_cpi_to_hours(cpi_yoy, cpi_mom, hourly_index):
    """
    Align CPI YoY and MoM series to the given hourly_index (trading hours).
    Forward-fill each CPI value from its release time onward until the next release.
    Returns a DataFrame indexed by hourly_index with CPI_YoY and CPI_MoM columns.
    """
    # Ensure hourly_index is a timezone-aware DatetimeIndex in UTC
    hourly_index = pd.to_datetime(hourly_index, errors="coerce")
    hourly_index = pd.DatetimeIndex(hourly_index)          # convert Series/list -> DatetimeIndex
    hourly_index = hourly_index.dropna()                   # remove any NaT entries
    if hourly_index.tz is None:
        hourly_index = hourly_index.tz_localize("UTC")
    else:
        hourly_index = hourly_index.tz_convert("UTC")

    # Determine release times for each CPI observation
    release_datetimes = get_cpi_release_schedule(cpi_yoy)
    # Ensure alignment of release list with values (they are in same order as cpi_yoy index)
    cpi_values = list(zip(cpi_yoy.values, cpi_mom.values))
    # Create a DataFrame to hold hourly values
    df = pd.DataFrame(index=hourly_index, columns=["CPI_YoY", "CPI_MoM"], dtype=float)
    for i, rel_time in enumerate(release_datetimes):
        yoy_val, mom_val = cpi_values[i]
        # Fill from this release time up to (but not including) the next release time
        if i < len(release_datetimes) - 1:
            next_rel_time = release_datetimes[i+1]
        else:
            next_rel_time = hourly_index[-1] + pd.Timedelta(hours=1)  # fill till end
        # Both df.index and rel_time/next_rel_time are UTC-aware, compare directly
        mask = (df.index >= rel_time) & (df.index < next_rel_time)
        # Assign values
        df.loc[mask, "CPI_YoY"] = yoy_val
        df.loc[mask, "CPI_MoM"] = mom_val
    # Forward fill any initial gaps (before first release in range, if any)
    df.fillna(method='ffill', inplace=True)
    return df

if __name__ == "__main__":
    # Fetch CPI index from FRED (seasonally adjusted CPI values)
    cpi_index = fetch_cpi_index(start_date="2022-01-01")
    # Compute YoY and MoM inflation rates (%)
    cpi_yoy, cpi_mom = compute_inflation_rates(cpi_index)
    # Load an example stock's hourly timestamps to align with (e.g., AAPL 1h data)
    stock_df = pd.read_csv(
        "data/raw/market/AAPL_1h.csv",
        skiprows=3,
        header=None,
        names=["Datetime", "Close", "High", "Low", "Open", "Volume"],
        dtype={"Close": float, "High": float, "Low": float, "Open": float, "Volume": float},
    )
    # Parse datetimes, coerce invalid strings to NaT, then drop those rows
    stock_df["Datetime"] = pd.to_datetime(stock_df["Datetime"], utc=True, errors="coerce", infer_datetime_format=True)
    stock_df = stock_df.dropna(subset=["Datetime"]).reset_index(drop=True)
    hourly_index = stock_df["Datetime"]
    # Align CPI data to the stock's hourly timeline
    cpi_hourly_df = align_cpi_to_hours(cpi_yoy, cpi_mom, hourly_index)
    # Save to CSV
    cpi_hourly_df.reset_index(inplace=True)
    cpi_hourly_df.rename(columns={"index": "Datetime"}, inplace=True)
    cpi_hourly_df.to_csv("CPI_hourly.csv", index=False)
    print("Saved CPI YoY and MoM data to CPI_hourly.csv")
