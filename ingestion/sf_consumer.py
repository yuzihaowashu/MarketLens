"""
Phase 2 — Kafka → Snowflake consumer.

Reads raw.stock.prices messages from Kafka and bulk-MERGEs them into
RAW_STOCK_PRICES.  Idempotent: replaying the same messages will
UPDATE existing rows rather than INSERT duplicates.

Usage (Phase 2 only — kafka-python must be installed):
    consumer = SnowflakePricesConsumer(conn, 'localhost:9092')
    n = consumer.run_batch(max_messages=500)

To enable:
    1. Uncomment kafka-python in requirements.txt
    2. Start Kafka: docker compose -f docker-compose.kafka.yml up -d
    3. Switch the Airflow DAG task to call sf_consumer instead of
       yfinance_producer.fetch_and_write_to_snowflake
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config as cfg

logger = logging.getLogger(__name__)


class SnowflakePricesConsumer:
    """
    Kafka consumer that writes raw.stock.prices messages to Snowflake.

    Each message value must be a JSON object matching UnifiedQuote.to_dict().
    The consumer uses 'marketlens-sf-writer' as the consumer group so
    Airflow can track offset progress independently of other consumers.
    """

    def __init__(self,
                 conn,
                 kafka_bootstrap: Optional[str] = None,
                 topic: Optional[str] = None):
        try:
            from kafka import KafkaConsumer
        except ImportError as exc:
            raise ImportError(
                'kafka-python is not installed. '
                'Uncomment kafka-python in requirements.txt and re-run pip install.'
            ) from exc

        self._conn  = conn
        self._topic = topic or cfg.KAFKA_PRICES_TOPIC
        self._consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=kafka_bootstrap or cfg.KAFKA_BOOTSTRAP,
            group_id='marketlens-sf-writer',
            auto_offset_reset='earliest',
            enable_auto_commit=False,      # we commit manually after SF write
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5_000,     # stop polling after 5s of silence
        )

    def run_batch(self, max_messages: int = 500) -> int:
        """
        Poll up to *max_messages* from Kafka, MERGE into Snowflake,
        then commit offsets.  Returns number of rows written.
        """
        batch: List[dict] = []
        for msg in self._consumer:
            batch.append(msg.value)
            if len(batch) >= max_messages:
                break

        if not batch:
            logger.info('No messages on topic %s', self._topic)
            return 0

        self._merge_batch(batch)
        self._consumer.commit()
        logger.info('Processed %d messages from %s', len(batch), self._topic)
        return len(batch)

    def _merge_batch(self, batch: List[dict]) -> None:
        """MERGE each message into RAW_STOCK_PRICES."""
        cursor = self._conn.cursor()
        merge_sql = """
            MERGE INTO SCORPION_DB.MARKETLENS.RAW_STOCK_PRICES AS tgt
            USING (
                SELECT
                    %(ticker)s   AS TICKER,
                    %(date)s     AS DATE,
                    %(open)s     AS OPEN_PRICE,
                    %(high)s     AS HIGH_PRICE,
                    %(low)s      AS LOW_PRICE,
                    %(close)s    AS CLOSE_PRICE,
                    %(volume)s   AS VOLUME,
                    %(source)s   AS SOURCE,
                    %(query_id)s AS QUERY_ID
            ) AS src ON (tgt.TICKER = src.TICKER
                     AND tgt.DATE   = src.DATE
                     AND tgt.SOURCE = src.SOURCE)
            WHEN MATCHED THEN UPDATE SET
                OPEN_PRICE  = src.OPEN_PRICE,
                HIGH_PRICE  = src.HIGH_PRICE,
                LOW_PRICE   = src.LOW_PRICE,
                CLOSE_PRICE = src.CLOSE_PRICE,
                VOLUME      = src.VOLUME,
                QUERY_ID    = src.QUERY_ID,
                INGESTED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
                 CLOSE_PRICE, VOLUME, SOURCE, QUERY_ID)
            VALUES
                (src.TICKER, src.DATE, src.OPEN_PRICE, src.HIGH_PRICE,
                 src.LOW_PRICE, src.CLOSE_PRICE, src.VOLUME,
                 src.SOURCE, src.QUERY_ID)
        """
        try:
            for row in batch:
                cursor.execute(merge_sql, row)
        finally:
            cursor.close()

    def close(self) -> None:
        self._consumer.close()
