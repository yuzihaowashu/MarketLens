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
SNOWFLAKE_USER = _env_strip('SNOWFLAKE_USER', 'GRIZZLY') or 'GRIZZLY'
# Will NEED TO CHANGE DEPENDING ON USER
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
        '/Users/andrewhaggstrom/Desktop/CS Projects/Keys/rsa_key.p8'
# Will need to change when running in your own set up
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
# FRED API (St. Louis Fed — https://fred.stlouisfed.org/docs/api/api_key.html)
# ---------------------------------------------------------------------------
FRED_API_KEY = _env_strip('FRED_API_KEY')

FRED_SERIES_GDP                    = 'GDPC1'    # Real GDP, quarterly
FRED_SERIES_HOUSING_STARTS         = 'HOUST'    # Housing starts, monthly SAAR
FRED_SERIES_CONSUMER_SENTIMENT     = 'UMCSENT'  # UMich consumer sentiment, monthly
FRED_SERIES_INFLATION_EXPECTATIONS = 'T10YIE'   # 10Y breakeven inflation, daily
FRED_SERIES_TREASURY_10Y           = 'DGS10'    # 10Y Treasury constant maturity yield, daily

# ---------------------------------------------------------------------------
# Signal thresholds
# ---------------------------------------------------------------------------
ANOMALY_Z_THRESHOLD = 2.0
ROLLING_WINDOW = 20

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
LLM_MODEL = 'llama3.1-70b'

# ---------------------------------------------------------------------------
# Config parsing helpers
# ---------------------------------------------------------------------------

def parse_env_bool(key: str, default: bool) -> bool:
    """Parse a boolean env var accepting 1/true/yes/on and 0/false/no/off."""
    val = os.environ.get(key, '').strip().lower()
    if val in ('1', 'true', 'yes', 'on'):
        return True
    if val in ('0', 'false', 'no', 'off'):
        return False
    return default


def parse_env_int(key: str, default: int,
                  min_val: Optional[int] = None,
                  max_val: Optional[int] = None) -> int:
    """Parse an integer env var with optional min/max clamping."""
    try:
        v = int(os.environ.get(key, str(default)))
    except (ValueError, TypeError):
        v = default
    if min_val is not None:
        v = max(min_val, v)
    if max_val is not None:
        v = min(max_val, v)
    return v


def parse_env_list(key: str, default: list) -> list:
    """Parse a comma-separated env var into a list of stripped strings."""
    raw = os.environ.get(key, '').strip()
    if not raw:
        return list(default)
    return [x.strip() for x in raw.split(',') if x.strip()]


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP     = _env_strip('KAFKA_BOOTSTRAP', 'localhost:9092') or 'localhost:9092'
KAFKA_PRICES_TOPIC  = _env_strip('KAFKA_PRICES_TOPIC', 'raw.stock.prices') or 'raw.stock.prices'
KAFKA_MACRO_TOPIC   = _env_strip('KAFKA_MACRO_TOPIC', 'raw.macro.indicators') or 'raw.macro.indicators'
KAFKA_SIGNALS_TOPIC = _env_strip('KAFKA_SIGNALS_TOPIC', 'signals.anomalies') or 'signals.anomalies'

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
MAX_WORKERS              = parse_env_int('MAX_WORKERS', 4, min_val=1, max_val=16)
FETCH_RETRY_ATTEMPTS     = parse_env_int('FETCH_RETRY_ATTEMPTS', 3, min_val=1, max_val=10)
CIRCUIT_BREAKER_FAILURES = parse_env_int('CIRCUIT_BREAKER_FAILURES', 3, min_val=1)
CIRCUIT_BREAKER_COOLDOWN = parse_env_int('CIRCUIT_BREAKER_COOLDOWN', 300, min_val=30)

# ---------------------------------------------------------------------------
# Notification
# ---------------------------------------------------------------------------
SLACK_WEBHOOK_URL = _env_strip('SLACK_WEBHOOK_URL')
ALERT_EMAIL       = _env_strip('ALERT_EMAIL')
SMTP_HOST         = _env_strip('SMTP_HOST', 'smtp.gmail.com') or 'smtp.gmail.com'
SMTP_PORT         = parse_env_int('SMTP_PORT', 587)
SMTP_USER         = _env_strip('SMTP_USER')
SMTP_PASSWORD     = _env_strip('SMTP_PASSWORD')

# ---------------------------------------------------------------------------
# SEC EDGAR
# ---------------------------------------------------------------------------
SEC_USER_AGENT           = _env_strip('SEC_USER_AGENT')
SEC_FORMS                = parse_env_list('SEC_FORMS', ['10-K', '10-Q'])
SEC_MAX_FILINGS_PER_RUN  = parse_env_int('SEC_MAX_FILINGS_PER_RUN', 20, min_val=1)
SEC_CHUNK_CHARS          = parse_env_int('SEC_CHUNK_CHARS', 6000, min_val=500)
SEC_SUMMARY_MODEL        = _env_strip('SEC_SUMMARY_MODEL', LLM_MODEL) or LLM_MODEL
SEC_DEBUG                = parse_env_bool('SEC_DEBUG', False)
SEC_REQUEST_SLEEP        = 0.11   # seconds between EDGAR requests (cap 10 req/s)

# ---------------------------------------------------------------------------
# Pipeline observability
# ---------------------------------------------------------------------------
PIPELINE_LOG_ENABLED = parse_env_bool('PIPELINE_LOG_ENABLED', True)
