"""
Simulated stock-price tick producer — Phase 2 Kafka streaming.

Seeds from the last known close price in Snowflake, then applies a
geometric Brownian motion random walk to generate realistic tick-by-tick
price moves for every watchlist ticker.

Publishes one round per ticker every TICK_INTERVAL_SEC seconds to the
raw.stock.prices Kafka topic.  All messages carry source='SIMULATED' so
they are never confused with real market data.

Usage:
    python -m ingestion.tick_producer
    TICK_INTERVAL_SEC=1 python -m ingestion.tick_producer
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, 'app')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config as cfg
from snowflake_client import run_query

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [tick_producer]: %(message)s')
logger = logging.getLogger('tick_producer')

TICK_INTERVAL_SEC = float(os.getenv('TICK_INTERVAL_SEC', '2'))
ANNUAL_VOL = 0.20          # 20 % annualised vol — realistic for large-caps
# fraction of a trading year per tick (252 days × 6.5 h × 3600 s)
DT = TICK_INTERVAL_SEC / (252 * 6.5 * 3600)

_FALLBACK_PRICES: dict[str, float] = {
    'AAPL': 213.0, 'MSFT': 415.0, 'GOOGL': 175.0, 'AMZN': 195.0,
    'TSLA': 248.0, 'NVDA': 875.0, 'META': 585.0, 'SPY': 520.0, 'QQQ': 445.0,
}


def _fetch_seed_prices() -> dict[str, float]:
    """Get the latest close price per ticker from Snowflake."""
    try:
        _, rows = run_query(
            "SELECT TICKER, CLOSE_PRICE FROM V_STOCK_PRICES "
            "WHERE CLOSE_PRICE IS NOT NULL "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY DATE DESC) = 1"
        )
        prices = {r[0]: float(r[1]) for r in rows if r[1] is not None}
    except Exception as e:
        logger.warning("Could not fetch seed prices from Snowflake (%s) — using fallbacks", e)
        prices = {}
    for ticker, fallback in _FALLBACK_PRICES.items():
        prices.setdefault(ticker, fallback)
    return prices


def _gbm_step(price: float) -> float:
    """One GBM step: S_{t+dt} = S_t · exp((−½σ²)dt + σ√dt · Z), Z~N(0,1)."""
    z = random.gauss(0, 1)
    return price * math.exp((-0.5 * ANNUAL_VOL ** 2) * DT + ANNUAL_VOL * math.sqrt(DT) * z)


def main() -> None:
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python not installed — run: pip install 'kafka-python>=2.0,<3'")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5,
    )

    logger.info("Fetching seed prices from Snowflake...")
    prices = _fetch_seed_prices()
    logger.info("Seeded %d tickers: %s", len(prices), sorted(prices))
    logger.info("Publishing to topic '%s' every %.1fs  (Ctrl-C to stop)",
                cfg.KAFKA_PRICES_TOPIC, TICK_INTERVAL_SEC)

    round_n = 0
    while True:
        round_n += 1
        ts = datetime.now(timezone.utc).isoformat()

        for ticker in cfg.WATCHLIST_TICKERS:
            prev  = prices.get(ticker, 100.0)
            curr  = _gbm_step(prev)
            prices[ticker] = curr

            producer.send(cfg.KAFKA_PRICES_TOPIC, value={
                'ticker':     ticker,
                'price':      round(curr, 4),
                'prev_price': round(prev, 4),
                'change_pct': round((curr - prev) / prev * 100, 4),
                'timestamp':  ts,
                'source':     'SIMULATED',
                'round':      round_n,
            })

        producer.flush()
        logger.info("Round %d — %d ticks published", round_n, len(cfg.WATCHLIST_TICKERS))
        time.sleep(TICK_INTERVAL_SEC)


if __name__ == '__main__':
    main()
