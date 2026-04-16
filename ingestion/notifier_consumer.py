"""
Notification consumer — Phase 2 Kafka streaming.

Reads anomaly events from signals.anomalies and broadcasts alerts via
Slack and email using the existing notification infrastructure.
Fail-open: a broken sender never crashes the consumer.

Consumer group: notifier  (independent offset)
"""

from __future__ import annotations

import json
import logging
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, 'app')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config as cfg

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [notifier_consumer]: %(message)s')
logger = logging.getLogger('notifier_consumer')


def _build_alert(data: dict) -> tuple[str, str]:
    ticker    = data['ticker']
    z         = data['z_score']
    price     = data['price']
    severity  = data.get('severity', 'MEDIUM')
    icon      = '🚨' if severity == 'HIGH' else '⚠️'
    direction = 'spike' if z > 0 else 'drop'
    title = f"{icon} {ticker} price {direction} detected — z-score {z:+.2f}"
    body  = (
        f"**{ticker}** moved to **${price:.2f}**\n"
        f"Z-score: {z:+.2f}  |  Severity: {severity}\n"
        f"Source: {data.get('source', 'SIMULATED')}  |  {data.get('timestamp', '')}"
    )
    return title, body


def main() -> None:
    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.error("kafka-python not installed — run: pip install 'kafka-python>=2.0,<3'")
        sys.exit(1)

    try:
        from notification.slack_sender import SlackSender
        from notification.email_sender import EmailSender
        from notification.base_sender import broadcast
        senders = [SlackSender(), EmailSender()]
        logger.info("Notification senders: Slack + Email")
    except Exception as e:
        logger.warning("Notification senders unavailable (%s) — alerts will be logged only", e)
        senders = []
        broadcast = None

    consumer = KafkaConsumer(
        cfg.KAFKA_SIGNALS_TOPIC,
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        group_id='notifier',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    logger.info("Started — group=notifier, reading '%s'", cfg.KAFKA_SIGNALS_TOPIC)

    for msg in consumer:
        data  = msg.value
        title, body = _build_alert(data)
        logger.warning("ALERT: %s", title)
        if senders and broadcast:
            try:
                broadcast(senders, title, body)
            except Exception as e:
                logger.error("Broadcast failed (non-fatal): %s", e)


if __name__ == '__main__':
    main()
