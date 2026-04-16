"""
Dashboard live-feed consumer — Phase 2 Kafka streaming.

Reads price ticks from raw.stock.prices and writes a compact JSON file
(live_feed.json at project root) that the Streamlit app polls every 2
seconds via @st.fragment(run_every=2).

Uses an atomic tmp-then-rename write so Streamlit never reads a
half-written file.

Consumer group: dashboard  (independent offset)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, 'app')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config as cfg

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [dashboard_consumer]: %(message)s')
logger = logging.getLogger('dashboard_consumer')

LIVE_FEED_PATH   = os.path.join(_ROOT, 'live_feed.json')
WRITE_EVERY_SEC  = 1.0    # write at most once per second to avoid I/O thrash


def _atomic_write(path: str, payload: dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def main() -> None:
    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.error("kafka-python not installed — run: pip install 'kafka-python>=2.0,<3'")
        sys.exit(1)

    consumer = KafkaConsumer(
        cfg.KAFKA_PRICES_TOPIC,
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        group_id='dashboard',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=500,   # unblock every 500 ms so we can write even if no new messages
    )

    state:      dict[str, dict] = {}
    last_write: float           = 0.0
    total_msgs: int             = 0

    logger.info("Started — group=dashboard, writing to %s", LIVE_FEED_PATH)

    while True:
        # Drain whatever is available (up to consumer_timeout_ms of silence)
        try:
            for msg in consumer:
                data   = msg.value
                ticker = data.get('ticker')
                if ticker:
                    state[ticker] = data
                    total_msgs   += 1
        except Exception:
            pass  # StopIteration from consumer_timeout — expected

        now = time.monotonic()
        if state and (now - last_write) >= WRITE_EVERY_SEC:
            _atomic_write(LIVE_FEED_PATH, {
                'updated_at':  datetime.now(timezone.utc).isoformat(),
                'total_msgs':  total_msgs,
                'tickers':     state,
            })
            last_write = now
            logger.debug("Wrote live_feed.json (%d tickers, %d total msgs)", len(state), total_msgs)


if __name__ == '__main__':
    main()
