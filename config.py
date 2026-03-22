import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
# Do not override variables already set in the shell (e.g. CI, Airflow).
load_dotenv(_PROJECT_ROOT / '.env')


def _env_strip(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return stripped env value, or *default* if missing or blank."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    s = raw.strip()
    return s if s else default


# ---------------------------------------------------------------------------
# Snowflake connection (override any value via environment variables)
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT = _env_strip('SNOWFLAKE_ACCOUNT', 'SFEDU02-UNB02139') or 'SFEDU02-UNB02139'
SNOWFLAKE_USER = _env_strip('SNOWFLAKE_USER', 'SCORPION') or 'SCORPION'
SNOWFLAKE_DATABASE = _env_strip('SNOWFLAKE_DATABASE', 'SCORPION_DB') or 'SCORPION_DB'
SNOWFLAKE_SCHEMA = _env_strip('SNOWFLAKE_SCHEMA', 'MARKETLENS') or 'MARKETLENS'
SNOWFLAKE_WAREHOUSE = _env_strip('SNOWFLAKE_WAREHOUSE', 'SCORPION_WH') or 'SCORPION_WH'
# Comma-separated names tried in order after SNOWFLAKE_WAREHOUSE if USE WAREHOUSE fails (02000/2043).
_fb = _env_strip('SNOWFLAKE_WAREHOUSE_FALLBACKS', 'SCORPION_WH') or 'SCORPION_WH'
SNOWFLAKE_WAREHOUSE_FALLBACKS = [x.strip() for x in _fb.split(',') if x.strip()]
SNOWFLAKE_ROLE = _env_strip('SNOWFLAKE_ROLE', 'TRAINING_ROLE') or 'TRAINING_ROLE'
_private_key_raw = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
if _private_key_raw:
    SNOWFLAKE_PRIVATE_KEY_PATH = os.path.expanduser(
        os.path.expandvars(_private_key_raw.strip())
    )
else:
    SNOWFLAKE_PRIVATE_KEY_PATH = os.path.expanduser(
        '~/airflow/snowflake_rsa_key.p8'
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
