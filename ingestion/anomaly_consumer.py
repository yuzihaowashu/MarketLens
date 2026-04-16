"""
Real-time anomaly detection consumer — Phase 2 Kafka streaming.

Reads price ticks from raw.stock.prices, maintains a 20-tick rolling window
per ticker in memory, computes a Z-score on each arrival, and publishes an
anomaly event to signals.anomalies whenever |Z| > ANOMALY_Z_THRESHOLD.

Consumer group: anomaly-check  (independent offset from all other consumers)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import deque

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, 'app')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config as cfg

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [anomaly_consumer]: %(message)s')
logger = logging.getLogger('anomaly_consumer')

WINDOW   = 20
Z_THRESH = cfg.ANOMALY_Z_THRESHOLD   # default 2.0


def _z_score(window: deque) -> float:
    n    = len(window)
    mean = sum(window) / n
    var  = sum((x - mean) ** 2 for x in window) / n
    std  = var ** 0.5
    return (window[-1] - mean) / std if std > 0 else 0.0


def main() -> None:
    try:
        from kafka import KafkaConsumer, KafkaProducer
    except ImportError:
        logger.error("kafka-python not installed — run: pip install 'kafka-python>=2.0,<3'")
        sys.exit(1)

    consumer = KafkaConsumer(
        cfg.KAFKA_PRICES_TOPIC,
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        group_id='anomaly-check',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )
    producer = KafkaProducer(
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,
    )

    windows: dict[str, deque] = {}

    logger.info("Started — group=anomaly-check, reading '%s', writing '%s'",
                cfg.KAFKA_PRICES_TOPIC, cfg.KAFKA_SIGNALS_TOPIC)

    for msg in consumer:
        data   = msg.value
        ticker = data.get('ticker')
        price  = data.get('price')
        if not ticker or price is None:
            continue

        w = windows.setdefault(ticker, deque(maxlen=WINDOW))
        w.append(price)

        if len(w) < 5:
            continue   # warm-up: need at least 5 ticks

        z = _z_score(w)
        if abs(z) <= Z_THRESH:
            continue

        anomaly = {
            'ticker':    ticker,
            'price':     price,
            'z_score':   round(z, 3),
            'mean':      round(sum(w) / len(w), 4),
            'severity':  'HIGH' if abs(z) > 3.0 else 'MEDIUM',
            'timestamp': data.get('timestamp'),
            'source':    data.get('source', 'SIMULATED'),
        }
        producer.send(cfg.KAFKA_SIGNALS_TOPIC, value=anomaly)
        producer.flush()
        logger.warning("ANOMALY  %s  price=%.2f  z=%+.2f  severity=%s",
                       ticker, price, z, anomaly['severity'])


if __name__ == '__main__':
    main()
