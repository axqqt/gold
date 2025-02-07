import os
import yfinance as yf
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import pytz
import requests
import logging
import schedule

# Load environment variables (if needed)
from dotenv import load_dotenv
load_dotenv()

class MarketStructureTracker:
    def __init__(self):
        # Hardcoded symbols: NASDAQ (^IXIC) and Gold (XAUUSD=X)
        self.symbols = ["^IXIC", "XAUUSD=X"]
        self.interval = os.getenv('INTERVAL', '15m')  # Default interval is 15 minutes
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK')
        self.setup_logging()
    
    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        # Add a console handler for real-time output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger().addHandler(console_handler)
    
    def fetch_data(self, symbol, periods=50):
        """Fetch historical data for a given symbol."""
        try:
            ticker = yf.Ticker(symbol.strip())
            data = ticker.history(period='1d', interval=self.interval)
            if data.empty:
                logging.warning(f"No data fetched for {symbol}")
                return None
            logging.info(f"Successfully fetched data for {symbol}")
            return data.tail(periods)
        except Exception as e:
            logging.error(f"Data fetch error for {symbol}: {e}")
            return None
    
    def detect_market_structure_shift(self, data):
        """Detect market structure shifts based on price action and moving averages."""
        if data is None or len(data) < 10:
            logging.warning("Insufficient data for analysis")
            return None
        
        highs = data['High'].values
        lows = data['Low'].values
        closes = data['Close'].values
        
        higher_high = highs[-1] > np.max(highs[:-1])
        lower_low = lows[-1] < np.min(lows[:-1])
        
        ma_short = np.mean(closes[-5:])
        ma_long = np.mean(closes[-10:])
        trend_change = abs(ma_short - ma_long) / ma_long > 0.005  # 0.5% threshold
        
        logging.info(f"Market structure analysis completed: Higher High={higher_high}, Lower Low={lower_low}, Trend Change={trend_change}")
        return {
            'higher_high': higher_high,
            'lower_low': lower_low,
            'trend_change': trend_change,
            'current_price': closes[-1],
            'ma_short': ma_short,
            'ma_long': ma_long
        }
    
    def send_discord_notification(self, symbol, message):
        """Send a notification to Discord."""
        if not self.discord_webhook:
            logging.warning("No Discord webhook configured")
            return
        
        payload = {"content": f"{symbol} Update:\n{message}"}
        try:
            response = requests.post(self.discord_webhook, json=payload)
            response.raise_for_status()
            logging.info(f"Discord notification sent for {symbol}")
        except Exception as e:
            logging.error(f"Discord notification error for {symbol}: {e}")
    
    def analyze_and_notify(self):
        """Analyze market structure shifts for all symbols and send notifications."""
        logging.info("Starting market structure analysis...")
        for symbol in self.symbols:
            logging.info(f"Processing symbol: {symbol}")
            data = self.fetch_data(symbol.strip())
            if data is not None:
                mss_result = self.detect_market_structure_shift(data)
                if mss_result:
                    if mss_result['higher_high'] or mss_result['lower_low'] or mss_result['trend_change']:
                        message = f"""🔔 Market Structure Shift Detected:
                        Current Price: ${mss_result['current_price']:.2f}
                        Higher High: {mss_result['higher_high']}
                        Lower Low: {mss_result['lower_low']}
                        Trend Change: {mss_result['trend_change']}
                        Short MA: {mss_result['ma_short']:.2f}
                        Long MA: {mss_result['ma_long']:.2f}"""
                        logging.info(f"[{symbol}] {message}")
                        self.send_discord_notification(symbol, message)
            else:
                logging.warning(f"Skipping analysis for {symbol} due to missing or invalid data")
        logging.info("Market structure analysis completed.")
    
    def run(self):
        """Schedule the analysis and notification process."""
        logging.info("Scheduling market structure analysis every 15 minutes...")
        schedule.every(15).minutes.do(self.analyze_and_notify)
        
        while True:
            logging.info("Running pending scheduled tasks...")
            schedule.run_pending()
            time.sleep(1)


def main():
    logging.info("Initializing Market Structure Tracker...")
    tracker = MarketStructureTracker()
    tracker.run()


if __name__ == '__main__':
    main()