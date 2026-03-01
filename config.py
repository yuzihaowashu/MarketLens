import os

# ---------------------------------------------------------------------------
# Snowflake connection (override any value via environment variables)
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', 'SFEDU02-UNB02139')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER', 'SCORPION')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'SCORPION_DB')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'MARKETLENS')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'SCORPION_WH')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', 'TRAINING_ROLE')
SNOWFLAKE_PRIVATE_KEY_PATH = os.environ.get(
    'SNOWFLAKE_PRIVATE_KEY',
    os.path.expanduser('~/airflow/snowflake_rsa_key.p8'),
)

# ---------------------------------------------------------------------------
# Marketplace references
# ---------------------------------------------------------------------------
MARKETPLACE_DB = 'SNOWFLAKE_PUBLIC_DATA_FREE'
MARKETPLACE_SCHEMA = 'PUBLIC_DATA_FREE'

# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------
WATCHLIST_TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'SPY', 'QQQ',
]

TICKER_NAMES = {
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'AMZN': 'Amazon', 'TSLA': 'Tesla', 'NVDA': 'NVIDIA',
    'META': 'Meta', 'SPY': 'S&P 500 ETF', 'QQQ': 'Nasdaq 100 ETF',
}

PULSE_TICKERS = ['SPY', 'QQQ', 'AAPL', 'TSLA', 'NVDA']

# ---------------------------------------------------------------------------
# Variable names in Marketplace data
# ---------------------------------------------------------------------------
STOCK_CLOSE_VARIABLE = 'post-market_close_adjusted'
STOCK_VOLUME_VARIABLE = 'nasdaq_volume'

MACRO_VARIABLES = {
    'EFFR_PCT': 'Federal Funds Rate',
    'EFFR_TARGET_RATE_TP': 'Fed Funds Target (Upper)',
}

CPI_VARIABLE = 'CPI:_All_items,_Seasonally_adjusted,_Monthly'

# ---------------------------------------------------------------------------
# Signal thresholds
# ---------------------------------------------------------------------------
ANOMALY_Z_THRESHOLD = 2.0
ROLLING_WINDOW = 20

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
LLM_MODEL = 'llama3.1-70b'
