import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import argparse

# ----------------------------------------
# Config
# ----------------------------------------

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"
FUNCTION_NAME = "NEWS_SENTIMENT"

# Magnificent 7
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NVDA"]

# Free tier is ~5 req/min → be conservative
DEFAULT_SLEEP_SECONDS = 15


# ----------------------------------------
# Environment & helpers
# ----------------------------------------

load_dotenv()
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not ALPHAVANTAGE_API_KEY:
    raise RuntimeError(
        "ALPHAVANTAGE_API_KEY not found. "
        "Add it to your .env or environment variables."
    )


def format_time_alpha(dt: datetime) -> str:
    """
    Convert a datetime to Alpha Vantage NEWS_SENTIMENT time format: YYYYMMDDTHHMM.
    Seconds are optional; we do not need them for windowing.
    Assumes dt is timezone-aware (UTC).
    """
    if dt.tzinfo is None:
        raise ValueError("format_time_alpha expects tz-aware datetime (UTC).")
    return dt.strftime("%Y%m%dT%H%M")


def call_news_sentiment(
    ticker: str,
    window_start: datetime,
    window_end: datetime,
    limit: int = 1000,
) -> dict:
    """
    Make a single Alpha Vantage NEWS_SENTIMENT API call for one ticker and time window.
    window_start and window_end must be tz-aware UTC datetimes.
    """
    params = {
        "function": FUNCTION_NAME,
        "tickers": ticker,
        "time_from": format_time_alpha(window_start),
        "time_to": format_time_alpha(window_end),
        "sort": "EARLIEST",  # so we can move forward in time safely
        "limit": str(limit),
        "apikey": ALPHAVANTAGE_API_KEY,
    }

    response = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Handle common error messages (rate limit, etc.)
    if isinstance(data, dict) and (
        "Note" in data
        or "Information" in data
        or "Error Message" in data
    ):
        raise RuntimeError(
            f"Alpha Vantage error for ticker={ticker}, "
            f"window={window_start.date()}–{window_end.date()}: {data}"
        )

    return data


def parse_news_feed(data: dict, ticker: str) -> list[dict]:
    """
    Parse the 'feed' from NEWS_SENTIMENT response and return
    a list of rows, each row is (article, ticker-specific sentiment).
    """
    feed = data.get("feed", [])
    rows = []

    for article in feed:
        # Article-level fields
        time_published_str = article.get("time_published")
        time_published = None
        if time_published_str:
            # usually YYYYMMDDTHHMMSS; seconds may sometimes be missing
            for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
                try:
                    dt = datetime.strptime(time_published_str, fmt)
                    # treat Alpha timestamps as UTC
                    time_published = dt.replace(tzinfo=timezone.utc)
                    break
                except Exception:
                    continue

        if time_published is None:
            # If we cannot parse, skip this article
            continue

        overall_score = article.get("overall_sentiment_score")
        overall_label = article.get("overall_sentiment_label")

        # Ticker-specific sentiment is a list of dicts; we only keep our ticker
        ticker_sents = article.get("ticker_sentiment", [])
        ticker_entry = None
        for ts in ticker_sents:
            if ts.get("ticker") == ticker:
                ticker_entry = ts
                break

        # It *should* be present, but be robust
        if ticker_entry is None:
            continue

        row = {
            "ticker": ticker,
            "time_published": time_published,
            "title": article.get("title"),
            "summary": article.get("summary"),
            "source": article.get("source"),
            "url": article.get("url"),

            "overall_sentiment_score": float(overall_score)
            if overall_score is not None
            else None,
            "overall_sentiment_label": overall_label,

            "ticker_relevance_score": float(ticker_entry.get("relevance_score"))
            if ticker_entry.get("relevance_score") is not None
            else None,
            "ticker_sentiment_score": float(ticker_entry.get("ticker_sentiment_score"))
            if ticker_entry.get("ticker_sentiment_score") is not None
            else None,
            "ticker_sentiment_label": ticker_entry.get("ticker_sentiment_label"),
        }

        rows.append(row)

    return rows


def fetch_news_sentiment_for_ticker(
    ticker: str,
    start_dt_utc: datetime,
    end_dt_utc: datetime,
    window_days: int = 7,
    sleep_seconds: int = DEFAULT_SLEEP_SECONDS,
) -> pd.DataFrame:
    """
    Fetch Alpha Vantage NEWS_SENTIMENT for a single ticker
    over [start_dt_utc, end_dt_utc] in rolling windows (UTC).

    Returns a pandas DataFrame with one row per (article, ticker sentiment).
    """
    if start_dt_utc.tzinfo is None or end_dt_utc.tzinfo is None:
        raise ValueError("start_dt_utc and end_dt_utc must be tz-aware (UTC).")

    all_rows: list[dict] = []

    window_start = start_dt_utc
    while window_start <= end_dt_utc:
        window_end = min(window_start + timedelta(days=window_days - 1), end_dt_utc)

        print(
            f"[{ticker}] Fetching window {window_start.date()} → {window_end.date()}..."
        )

        try:
            data = call_news_sentiment(
                ticker=ticker,
                window_start=window_start,
                window_end=window_end,
                limit=1000,
            )
        except Exception as e:
            print(
                f"[{ticker}] Error in window {window_start.date()}–"
                f"{window_end.date()}: {e}"
            )
            # You may choose to break, retry, or skip; here we skip and continue
            window_start = window_end + timedelta(days=1)
            time.sleep(sleep_seconds)
            continue

        rows = parse_news_feed(data, ticker)
        print(f"[{ticker}] Retrieved {len(rows)} articles in this window.")

        all_rows.extend(rows)

        # Move to next window
        window_start = window_end + timedelta(days=1)

        # Respect API rate limits
        time.sleep(sleep_seconds)

    if not all_rows:
        print(f"[{ticker}] No articles found for the given period.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Basic cleaning / sorting
    if "time_published" in df.columns:
        df = df.sort_values("time_published").reset_index(drop=True)

    return df


def fetch_news_for_tickers(
    tickers: list[str],
    start_dt_utc: datetime,
    end_dt_utc: datetime,
    window_days: int = 7,
    sleep_seconds: int = DEFAULT_SLEEP_SECONDS,
) -> pd.DataFrame:
    """
    Convenience wrapper for multiple tickers. Concatenates all results.
    """
    dfs = []
    for t in tickers:
        df_t = fetch_news_sentiment_for_ticker(
            ticker=t,
            start_dt_utc=start_dt_utc,
            end_dt_utc=end_dt_utc,
            window_days=window_days,
            sleep_seconds=sleep_seconds,
        )
        if not df_t.empty:
            dfs.append(df_t)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    return combined


def aggregate_to_hourly(
    df_articles: pd.DataFrame,
    start_dt_utc: datetime,
    end_dt_utc: datetime,
) -> pd.DataFrame:
    """
    Aggregate article-level sentiment to hourly resolution per ticker.

    Output columns:
        Datetime (UTC, hourly),
        ticker,
        news_count,
        overall_sentiment_mean,
        ticker_sentiment_mean,
        ticker_relevance_mean
    """
    if df_articles.empty:
        return pd.DataFrame(
            columns=[
                "Datetime",
                "ticker",
                "news_count",
                "overall_sentiment_mean",
                "ticker_sentiment_mean",
                "ticker_relevance_mean",
            ]
        )

    df = df_articles.copy()

    # Ensure time_published is datetime with UTC tz
    df["time_published"] = pd.to_datetime(df["time_published"], utc=True)

    # Set as index for resampling
    df = df.set_index("time_published")

    # Aggregation per ticker & hour
    # 'url' is used just to count number of articles in the hour
    hourly = (
        df.groupby("ticker")
        .resample("1H")
        .agg(
            {
                "ticker_sentiment_score": "mean",
                "overall_sentiment_score": "mean",
                "ticker_relevance_score": "mean",
                "url": "count",
            }
        )
        .rename(
            columns={
                "ticker_sentiment_score": "ticker_sentiment_mean",
                "overall_sentiment_score": "overall_sentiment_mean",
                "ticker_relevance_score": "ticker_relevance_mean",
                "url": "news_count",
            }
        )
    )

    # We now have a MultiIndex (ticker, Datetime). We want a regular hourly grid
    # like your CPI file, but per ticker.

    # Build the full hourly range in UTC
    # Align to full hours between start & end
    start_hour = start_dt_utc.replace(minute=0, second=0, microsecond=0)
    end_hour = end_dt_utc.replace(minute=0, second=0, microsecond=0)

    all_hours = pd.date_range(
        start=start_hour, end=end_hour, freq="1H", tz=timezone.utc
    )

    frames = []
    tickers = hourly.index.get_level_values(0).unique()

    for t in tickers:
        # Subset for this ticker
        h_t = hourly.loc[t] if (t in hourly.index.get_level_values(0)) else pd.DataFrame()

        # Reindex to full hourly range
        h_t = h_t.reindex(all_hours)

        # Fill news_count with 0 where no articles in that hour
        if "news_count" in h_t.columns:
            h_t["news_count"] = h_t["news_count"].fillna(0).astype(int)
        else:
            h_t["news_count"] = 0

        # Sentiment/relevance means: keep NaN when no news (you can later
        # choose to forward-fill or replace NaN with 0 in your modeling step)
        h_t["ticker"] = t

        h_t = h_t.reset_index().rename(columns={"index": "Datetime"})
        frames.append(h_t)

    if not frames:
        return pd.DataFrame(
            columns=[
                "Datetime",
                "ticker",
                "news_count",
                "overall_sentiment_mean",
                "ticker_sentiment_mean",
                "ticker_relevance_mean",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)

    # Sort for cleanliness
    combined = combined.sort_values(["ticker", "Datetime"]).reset_index(drop=True)

    return combined[
        [
            "Datetime",
            "ticker",
            "news_count",
            "overall_sentiment_mean",
            "ticker_sentiment_mean",
            "ticker_relevance_mean",
        ]
    ]


# ----------------------------------------
# CLI entry point
# ----------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Alpha Vantage NEWS_SENTIMENT for given tickers and date range, "
            "aggregate to hourly, and save as a CSV aligned to a regular 1H grid."
        )
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated list of tickers (default: Magnificent 7).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2023-01-01",
        help="Start date (YYYY-MM-DD, UTC assumed), default: 2023-01-01.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD, UTC assumed), default: today (UTC).",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Size of each time window in days for API calls (default: 7).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=DEFAULT_SLEEP_SECONDS,
        help="Sleep seconds between API calls (default: 15).",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="alpha_news_sentiment_1h_M7_2023_onwards.csv",
        help="Output CSV path (hourly aggregated).",
    )

    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    # Parse dates as UTC
    start_dt_utc = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end_dt_utc = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )

    print(
        f"Fetching NEWS_SENTIMENT for tickers={tickers}, "
        f"{start_dt_utc.date()} → {end_dt_utc.date()} (UTC), "
        f"and aggregating to 1H into {args.output_path}"
    )

    # 1) Fetch article-level sentiment
    df_articles = fetch_news_for_tickers(
        tickers=tickers,
        start_dt_utc=start_dt_utc,
        end_dt_utc=end_dt_utc,
        window_days=args.window_days,
        sleep_seconds=args.sleep_seconds,
    )

    if df_articles.empty:
        print("No article-level data fetched. Nothing to aggregate or save.")
        return

    # 2) Aggregate to hourly
    df_hourly = aggregate_to_hourly(
        df_articles=df_articles,
        start_dt_utc=start_dt_utc,
        end_dt_utc=end_dt_utc,
    )

    if df_hourly.empty:
        print("Hourly aggregation produced no rows. Nothing to save.")
        return

    # 3) Save to CSV
    df_hourly.to_csv(args.output_path, index=False)
    print(f"Saved {len(df_hourly)} rows to {args.output_path}")


if __name__ == "__main__":
    main()
