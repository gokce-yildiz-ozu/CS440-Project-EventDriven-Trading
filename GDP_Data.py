import os
import requests
import pandas as pd
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()  # loads .env in working directory

FRED_API_KEY = os.getenv("FRED_API_KEY")
assert FRED_API_KEY, "Missing FRED_API_KEY in environment/.env"

# Use your existing GDP growth series (you were already using BEA account A191RL)
SERIES_ID = "A191RL1Q225SBEA"  # Real GDP: % change from preceding period, SAAR

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

def fetch_gdp_initial_release(series_id=SERIES_ID, observation_start="2010-01-01"):
    """
    Pull initial-release-only observations from FRED/ALFRED safely by specifying
    a wide realtime window.
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",

        # CRITICAL: wide realtime window so vintage data exists
        "realtime_start": "1900-01-01",
        "realtime_end": "9999-12-31",

        # Initial release only
        "output_type": 4,

        # Optional: reduce payload size
        "observation_start": observation_start,
    }

    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        print("FRED request failed:", r.status_code)
        print("URL:", r.url)
        print("Response body:\n", r.text)
    r.raise_for_status()

    data = r.json()["observations"]
    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["realtime_start"] = pd.to_datetime(df["realtime_start"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "realtime_start", "value"]).sort_values(["date", "realtime_start"])
    return df[["date", "realtime_start", "value"]]

def build_gdp_release_timestamps(gdp_df):
    """
    Create a proper release datetime at 08:30 ET on realtime_start, then convert to UTC.
    """
    gdp_df = gdp_df.copy()
    gdp_df["realtime_start"] = pd.to_datetime(gdp_df["realtime_start"], errors="coerce")
    gdp_df = gdp_df.dropna(subset=["realtime_start", "value"])

    # 08:30 ET on realtime_start date
    release_local = (
        gdp_df["realtime_start"].dt.tz_localize(NY).dt.normalize()
        + pd.Timedelta(hours=8, minutes=30)
    )

    gdp_df["Datetime"] = release_local.dt.tz_convert(UTC)
    gdp_df["GDP_Growth_QoQ"] = pd.to_numeric(gdp_df["value"], errors="coerce")
    gdp_df = gdp_df.dropna(subset=["GDP_Growth_QoQ"]) 
    return gdp_df[["Datetime", "GDP_Growth_QoQ"]].sort_values("Datetime")

def align_to_hourly(gdp_release_df, hourly_index_utc):
    """
    Align GDP releases to an hourly trading index using merge_asof:
    For each trading hour, take the most recent GDP release at or before that time.
    """
    timeline = pd.DataFrame({"Datetime": pd.to_datetime(hourly_index_utc, utc=True, errors="coerce")})
    timeline = timeline.dropna().sort_values("Datetime")

    gdp_release_df = gdp_release_df.copy()
    gdp_release_df["Datetime"] = pd.to_datetime(gdp_release_df["Datetime"], utc=True, errors="coerce")
    gdp_release_df = gdp_release_df.dropna(subset=["Datetime"]).sort_values("Datetime")

    out = pd.merge_asof(
        timeline,
        gdp_release_df[["Datetime", "GDP_Growth_QoQ"]],
        on="Datetime",
        direction="backward"  # take latest release <= hour
    )

    # Safety fill: if timeline starts before first release
    out["GDP_Growth_QoQ"] = out["GDP_Growth_QoQ"].ffill().bfill()
    return out

if __name__ == "__main__":
    gdp_initial = fetch_gdp_initial_release()

    # keep one row per observation date (initial release)
    gdp_initial = gdp_initial.sort_values(["date", "realtime_start"]).drop_duplicates(subset=["date"], keep="first")

    gdp_release = build_gdp_release_timestamps(gdp_initial)

    # Debug prints: distinct timestamps and head/tail
    print("Distinct release timestamps:", gdp_release["Datetime"].nunique())
    print("Release head:\n", gdp_release.head(10))
    print("Release tail:\n", gdp_release.tail(10))
    assert len(gdp_release) > 0, "gdp_release is empty; API returned no usable observations."

    # Use your stock timeline (or better: the full_index you already created)
    stock_df = pd.read_csv("data/raw/market/AAPL_1h.csv", skiprows=3,
                           names=["Datetime","Close","High","Low","Open","Volume"])
    stock_df["Datetime"] = pd.to_datetime(stock_df["Datetime"], utc=True, errors="coerce")
    stock_df = stock_df.dropna(subset=["Datetime"]).sort_values("Datetime")

    gdp_hourly = align_to_hourly(gdp_release, stock_df["Datetime"])
    gdp_hourly.to_csv("GDP_hourly.csv", index=False)
    print("Saved leakage-safe GDP to GDP_hourly.csv")
