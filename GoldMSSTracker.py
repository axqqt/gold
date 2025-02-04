import yfinance as yf
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import pytz
import requests
import logging
import schedule

class GoldMSSTracker:
    def __init__(self, symbol='XAUUSD=X', interval='15m', discord_webhook=None):
        self.symbol = symbol
        self.interval = interval
        self.discord_webhook = discord_webhook
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s'
        )

    def fetch_data(self, periods=50):
        try:
            ticker = yf.Ticker(self.symbol)
            data = ticker.history(period='1d', interval=self.interval)
            return data.tail(periods)
        except Exception as e:
            logging.error(f"Data fetch error: {e}")
            return None

    def detect_market_structure_shift(self, data):
        if data is None or len(data) < 10:
            return None

        highs = data['High'].values
        lows = data['Low'].values
        closes = data['Close'].values

        # Detect Higher High/Lower Low shifts
        higher_high = highs[-1] > np.max(highs[:-1])
        lower_low = lows[-1] < np.min(lows[:-1])

        # Trend change detection via moving averages
        ma_short = np.mean(closes[-5:])
        ma_long = np.mean(closes[-10:])
        trend_change = abs(ma_short - ma_long) / ma_long > 0.005

        return {
            'higher_high': higher_high,
            'lower_low': lower_low,
            'trend_change': trend_change,
            'current_price': closes[-1],
            'ma_short': ma_short,
            'ma_long': ma_long
        }

    def send_discord_notification(self, message):
        if not self.discord_webhook:
            logging.warning("No Discord webhook configured")
            return

        payload = {"content": message}
        try:
            response = requests.post(self.discord_webhook, json=payload)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Discord notification error: {e}")

    def analyze_and_notify(self):
        data = self.fetch_data()
        if data is not None:
            mss_result = self.detect_market_structure_shift(data)
            if mss_result:
                if mss_result['higher_high'] or mss_result['lower_low'] or mss_result['trend_change']:
                    message = f"""🔔 Market Structure Shift Detected for Gold:
                    Current Price: ${mss_result['current_price']:.2f}
                    Higher High: {mss_result['higher_high']}
                    Lower Low: {mss_result['lower_low']}
                    Trend Change: {mss_result['trend_change']}
                    Short MA: {mss_result['ma_short']:.2f}
                    Long MA: {mss_result['ma_long']:.2f}"""
                    logging.info(message)
                    self.send_discord_notification(message)

    def run(self):
        # Schedule daily reset at midnight New York time
        schedule.every().day.at("00:00").do(self.reset)
        
        # Run analysis every 15 minutes
        schedule.every(15).minutes.do(self.analyze_and_notify)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def reset(self):
        logging.info("Daily reset performed")

if __name__ == '__main__':
    # Replace with your actual Discord webhook URL
    DISCORD_WEBHOOK = "YOUR_DISCORD_WEBHOOK_URL"
    
    tracker = GoldMSSTracker(discord_webhook=DISCORD_WEBHOOK)
    tracker.run()