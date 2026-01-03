import os
from dotenv import load_dotenv
import pandas_datareader.data as web
from datetime import datetime
import pandas as pd

load_dotenv()
fred_key = os.getenv("FRED_API_KEY")

# Fetch the Federal Funds Rate data from FRED 
start_date = datetime(2023, 12, 6) 
fedfunds = web.DataReader("FEDFUNDS", "fred", start=start_date, api_key=fred_key)

# Rename and clean up
fedfunds = fedfunds.rename(columns={"FEDFUNDS": "fed_funds_rate"})
fedfunds.index.name = "timestamp"

# Resample to hourly frequency (forward-fill from rate announcement dates)
fedfunds_hourly = fedfunds.resample("H").ffill()

# Save to CSV
output_path = "data/raw/macro/fedfunds_hourly.csv"
fedfunds_hourly.to_csv(output_path)

print(f"FEDFUNDS data saved to {output_path}")
