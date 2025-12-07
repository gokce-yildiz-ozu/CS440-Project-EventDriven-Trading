import yfinance as yf
import pandas as pd
import os
from datetime import datetime

#yahoo sadece 2 yillik veri tutuyormus ondan son iki yili aliyorum
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"] # Magnificent 7
START_DATE = "2023-01-01"
END_DATE = datetime.today().strftime('%Y-%m-%d')

MARKET_DATA_PATH = "data/raw/market"
CORPORATE_DATA_PATH = "data/raw/corporate"

def create_directories():
    os.makedirs(MARKET_DATA_PATH, exist_ok=True)
    os.makedirs(CORPORATE_DATA_PATH, exist_ok=True)

def get_market_data():
    """Hisse fiyatlarini intraday ceker"""
    print(f"\n{len(TICKERS)}")
    
    for ticker in TICKERS:
        try:
            # 1 saatlik veriyi son 730 güne kadar (2 yıl) çekebilirsin
            # period="2y" diyerek maksimum geçmişi alıyoruz
            df = yf.download(ticker, period="2y", interval="1h", progress=False)
            
            if not df.empty:
                save_path = f"{MARKET_DATA_PATH}/{ticker}_1h.csv"
                df.to_csv(save_path)
                print(f"   Saved: {ticker} ({len(df)} satır)")
            else:
                print(f"   Warning: {ticker} is empty")
        except Exception as e:
            print(f"   Error ({ticker}): {e}")

def get_earnings_data():
    """Şirketlerin bilanço (Earnings) tarihlerini ve tahminlerini çeker."""
    print(f"\n Corporate Earnings Datas")
    
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            
            # 1. Gelecek Earnings Tarihleri (Calendar)
            calendar = stock.calendar
            if calendar:
                # Calendar bazen Dictionary bazen DataFrame dönebilir, kontrol ediyoruz
                if isinstance(calendar, dict):
                     calendar_df = pd.DataFrame(calendar)
                else:
                     calendar_df = calendar
                
                cal_path = f"{CORPORATE_DATA_PATH}/{ticker}_calendar.csv"
                calendar_df.to_csv(cal_path)
                print(f"   Saved Calendar: {ticker}")
            
            # 2. Geçmiş Yıllık/Çeyreklik Bilançolar (Income Statement)
            # Bu bize Revenue ve Net Income bilgisini verir
            income = stock.quarterly_income_stmt
            if not income.empty:
                inc_path = f"{CORPORATE_DATA_PATH}/{ticker}_financials.csv"
                income.to_csv(inc_path)
                print(f"   Saved Financials: {ticker}")

        except Exception as e:
            print(f"   Error ({ticker} Earnings): {e}")

# --- ANA ÇALIŞTIRMA BLOKU ---
if __name__ == "__main__":
    print(" Veri Toplama Motoru Başlatıldı...")
    create_directories()
    get_market_data()
    get_earnings_data()
    print("\nDone!")