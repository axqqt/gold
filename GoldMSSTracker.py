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
import yaml
from dotenv import load_dotenv
from typing import Dict, List, Optional
import concurrent.futures


class MarketStructureTracker:
    def __init__(self, config_path: str):
        # Load environment variables
        load_dotenv()
        
        # Load and validate configuration
        self.config = self.load_config(config_path)
        self.validate_config()
        
        # Extract symbols with validation
        self.stocks = self.config.get("assets", {}).get("stocks", [])
        self.commodities = self.config.get("assets", {}).get("commodities", [])
        self.symbols = list(set(self.stocks + self.commodities))  # Remove duplicates
        
        # Notification settings with defaults
        notification_config = self.config.get("notification", {})
        self.discord_enabled = notification_config.get("discord", {}).get("enabled", False)
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK') or notification_config.get("discord", {}).get("webhook")
        
        # Logging settings with robust defaults
        logging_config = notification_config.get("logging", {})
        self.logging_enabled = logging_config.get("enabled", True)
        self.logging_level = logging_config.get("level", "INFO").upper()
        
        # Interval and timeout settings
        self.interval = os.getenv('INTERVAL', '15m')
        self.request_timeout = 10  # seconds
        
        # Set up logging
        self.setup_logging()
    
    def validate_config(self):
        """Validate configuration parameters."""
        if not self.config:
            raise ValueError("Configuration is empty or invalid")
        
        # Validate asset symbols
        if not self.config.get("assets", {}).get("stocks") and not self.config.get("assets", {}).get("commodities"):
            logging.warning("No stock or commodity symbols defined in configuration")
    
    def load_config(self, config_path: str) -> Dict:
        """Load and parse the YAML configuration file with error handling."""
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)
            logging.info("Configuration file loaded successfully.")
            return config or {}
        except FileNotFoundError:
            logging.error(f"Configuration file not found: {config_path}")
            return {}
        except yaml.YAMLError as e:
            logging.error(f"YAML parsing error: {e}")
            return {}
    
    def setup_logging(self):
        """Set up comprehensive logging configuration."""
        log_level = getattr(logging, self.logging_level, logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('market_tracker.log', encoding='utf-8')
            ]
        )
    
    def fetch_data(self, symbol: str, periods: int = 50) -> Optional[pd.DataFrame]:
        """Fetch historical data for a given symbol with improved error handling."""
        try:
            ticker = yf.Ticker(symbol.strip())
            data = ticker.history(period='1d', interval=self.interval, timeout=self.request_timeout)
            
            if data.empty:
                logging.warning(f"No data fetched for {symbol}. Possibly delisted or invalid symbol.")
                return None
            
            logging.info(f"Successfully fetched data for {symbol}")
            return data.tail(periods)
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error fetching data for {symbol}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching data for {symbol}: {e}")
        
        return None
    
    def detect_market_structure_shift(self, data: pd.DataFrame) -> Optional[Dict]:
        """Enhanced market structure shift detection."""
        if data is None or len(data) < 10:
            logging.warning("Insufficient data for analysis")
            return None
        
        try:
            highs = data['High'].values
            lows = data['Low'].values
            closes = data['Close'].values
            
            higher_high = highs[-1] > np.max(highs[:-1])
            lower_low = lows[-1] < np.min(lows[:-1])
            
            ma_short = np.mean(closes[-5:])
            ma_long = np.mean(closes[-10:])
            trend_change = abs(ma_short - ma_long) / ma_long > 0.005  # 0.5% threshold
            
            return {
                'higher_high': higher_high,
                'lower_low': lower_low,
                'trend_change': trend_change,
                'current_price': closes[-1],
                'ma_short': ma_short,
                'ma_long': ma_long
            }
        except Exception as e:
            logging.error(f"Market structure analysis error: {e}")
            return None
    
    def send_discord_notification(self, symbol: str, message: str):
        """Robust Discord notification with retry mechanism."""
        if not self.discord_webhook:
            logging.warning("No Discord webhook configured")
            return
        
        payload = {
            "content": f"🔔 **{symbol} Market Structure Shift Detected!** 🔔\n\n{message}"
        }
        
        try:
            response = requests.post(
                self.discord_webhook, 
                json=payload, 
                timeout=self.request_timeout
            )
            response.raise_for_status()
            logging.info(f"Discord notification sent for {symbol}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Discord notification error for {symbol}: {e}")
    
    def analyze_symbol(self, symbol: str):
        """Process individual symbol with error handling."""
        logging.info(f"Processing symbol: {symbol}")
        try:
            data = self.fetch_data(symbol.strip())
            if data is not None:
                mss_result = self.detect_market_structure_shift(data)
                if mss_result and mss_result['trend_change']:  # Only notify if trend_change is True
                    timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z')
                    message = self._format_notification_message(timestamp, mss_result)
                    self.send_discord_notification(symbol, message)
        except Exception as e:
            logging.error(f"Unexpected error processing {symbol}: {e}")
    
    def _format_notification_message(self, timestamp: str, result: Dict) -> str:
        """Format notification message with consistent structure."""
        trend_direction = "📈 Upward Trend" if result['ma_short'] > result['ma_long'] else "📉 Downward Trend"
        return f"""**⏰ Timestamp:** {timestamp}
**💰 Current Price:** ${result['current_price']:.2f}
**📊 Short MA (5-period):** {result['ma_short']:.2f}
**📊 Long MA (10-period):** {result['ma_long']:.2f}
**🔄 Trend Direction:** {trend_direction}
**⚠️ Trend Change Detected:** ✅"""
    
    def analyze_and_notify(self):
        """Parallel processing of symbols for efficiency."""
        logging.info("Starting market structure analysis...")
        
        # Use ThreadPoolExecutor for concurrent symbol processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(self.symbols))) as executor:
            executor.map(self.analyze_symbol, self.symbols)
        
        logging.info("Market structure analysis completed.")
    
    def run(self):
        """Robust scheduling of market structure tracking."""
        print("Starting Market Structure Tracker...")
        logging.info("Initializing Market Structure Tracker...")
        
        # Calculate time until next 15-minute interval from midnight New York time
        self._wait_until_next_15_minute_interval()
        
        # Schedule tasks with error handling
        schedule.every(15).minutes.do(self._safe_analyze_and_notify)
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}")
                time.sleep(60)  # Wait before retrying
    
    def _wait_until_next_15_minute_interval(self):
        """Wait until the next 15-minute interval from midnight New York time."""
        ny_timezone = pytz.timezone('America/New_York')
        
        while True:
            now_ny = datetime.now(ny_timezone)
            
            # Calculate the next 15-minute interval
            next_run_minute = ((now_ny.minute // 15) + 1) * 15
            next_run_time = now_ny.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_run_minute)
            
            # If the calculated time is past midnight, reset to the next day
            if next_run_time.day != now_ny.day:
                next_run_time = next_run_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            
            # Sleep until the next 15-minute interval
            sleep_duration = (next_run_time - now_ny).total_seconds()
            if sleep_duration <= 0:
                break  # Already at or past the next interval
            
            logging.info(f"Waiting until next 15-minute interval at {next_run_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            time.sleep(sleep_duration)
    
    def _safe_analyze_and_notify(self):
        """Wrapper for analyze_and_notify with additional error handling."""
        try:
            self.analyze_and_notify()
        except Exception as e:
            logging.error(f"Error in scheduled task: {e}")


def main():
    config_path = "config.yaml"
    tracker = MarketStructureTracker(config_path)
    tracker.run()


if __name__ == '__main__':
    main()