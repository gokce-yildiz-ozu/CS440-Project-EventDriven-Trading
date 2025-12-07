from sec_edgar_downloader import Downloader
import os


USER_EMAIL = "gokce.yildiz@ozu.edu.tr"  
COMPANY_NAME = "Ozyegin University"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
TEXT_DATA_PATH = "data/raw/text"

def fetch_sec_reports():
    print(f"SEC Reports (Recording at: {TEXT_DATA_PATH})")
    
    os.makedirs(TEXT_DATA_PATH, exist_ok=True)
    
    dl = Downloader(COMPANY_NAME, USER_EMAIL, TEXT_DATA_PATH)
    
    for ticker in TICKERS:
        try:
            print(f"\n Searching reports for {ticker}")
            
            # DUZELTME: 'amount' yerine 'limit' kullaniyoruz (v5.0+)
            
            # 1. 10-K Report (yearly)
            dl.get("10-K", ticker, limit=1)
            
            # 2. 10-Q report (quarterly)
            dl.get("10-Q", ticker, limit=2)
            
        except Exception as e:
            print(f"   Error ({ticker}): {e}")

if __name__ == "__main__":
    fetch_sec_reports()
    print("\nDone")