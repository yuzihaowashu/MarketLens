# MarketLens — Detailed Implementation Plan

## 1. Project Overview

MarketLens is a **server-level financial data engineering platform** for US equity analysis. It ingests stock prices and macro indicators, computes trading signals inside Snowflake, and surfaces anomalies and summaries through a Streamlit dashboard.

Unlike lightweight single-process tools (e.g. `daily_stock_analysis`), MarketLens separates concerns across dedicated infrastructure layers: **Kafka** for event streaming, **Airflow** for orchestration, **Snowflake** as the analytical warehouse, and optionally **Spark/Flink** for distributed stream processing.

---

## 2. Current State

| Component | File(s) | Status |
|---|---|---|
| Snowflake schema + base views | `setup.sql` | Done |
| Signal SQL views (returns, vol, anomaly, macro) | `signals/01–05.sql` | Done |
| Snowflake Python client (RSA key auth) | `app/snowflake_client.py` | Done |
| Streamlit dashboard | `app/app.py` | Exists, scope TBD |
| Airflow DAG | `dags/marketlens_heartbeat.py` | Stub only (ping task) |
| Airflow standalone launcher | `scripts/airflow_standalone.sh` | Done |
| Kafka | — | Not yet started |
| Spark / Flink | — | Not yet started |
| Data ingestion from live APIs | — | Not yet started |

---

## 3. Target Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                  │
│                                                                   │
│  YFinanceFetcher ──┐                                             │
│  AlpacaFetcher ────┼──► Kafka Topic: raw.stock.prices            │
│  (macro: FRED) ────┘                                             │
│                         Kafka Topic: raw.macro.indicators        │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  PROCESSING LAYER  (Phase 2: Flink or Spark Structured           │
│                     Streaming; Phase 1: direct to Snowflake)     │
│                                                                   │
│  Consume raw.stock.prices                                        │
│  → normalize to UnifiedQuote schema                              │
│  → write to Snowflake staging table RAW_STOCK_PRICES             │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  STORAGE + ANALYTICS LAYER  (Snowflake, MARKETLENS schema)       │
│                                                                   │
│  RAW_STOCK_PRICES  ──► V_DAILY_RETURNS                           │
│                    ──► V_ROLLING_VOLATILITY                      │
│                    ──► V_ANOMALY_SCORES                          │
│  RAW_MACRO         ──► V_FED_RATE_CHANGES, V_CPI_CHANGES         │
│                    ──► V_SIGNAL_SUMMARY                          │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION LAYER  (Airflow)                                  │
│                                                                   │
│  marketlens_daily DAG                                            │
│  Task 1: ingest_prices   → fetch + publish to Kafka              │
│  Task 2: ingest_macro    → fetch + publish to Kafka              │
│  Task 3: consume_to_sf   → Kafka consumer writes to Snowflake    │
│  Task 4: refresh_signals → execute signal SQL views              │
│  Task 5: run_anomaly_check → query V_SIGNAL_SUMMARY              │
│  Task 6: notify          → log / alert on anomalies found        │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  SERVING LAYER  (Streamlit)                                      │
│                                                                   │
│  Dashboard: signal summary, anomaly table, macro overlay         │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Design Decisions Inspired by `daily_stock_analysis`

`daily_stock_analysis` solves the hardest part of ingestion — multi-source fallback, normalization, and retry — in its `data_provider/` layer. MarketLens borrows these patterns and promotes them to the Kafka producer tier.

| Pattern from `daily_stock_analysis` | MarketLens adaptation |
|---|---|
| `BaseFetcher` abstract class with `get_daily_data()` / `get_realtime_quote()` | `BaseProducer` abstract class with `fetch_and_publish(topic)` |
| Priority-ordered fallback chain (efinance → akshare → yfinance) | Producer fallback: YFinance (primary) → Alpaca (secondary); same Kafka topic regardless of source |
| `UnifiedRealtimeQuote` normalized struct | Kafka message schema (JSON or Avro) with fixed fields: `ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `source` |
| `tenacity` retry with exponential backoff on fetch | Same retry decorator on each producer's `fetch_and_publish()` |
| Source tag on each record (`RealtimeSource.STOOQ`, etc.) | `source` field in Kafka message so Snowflake can track data lineage |

---

## 5. Phased Implementation Plan

### Phase 0 — Foundation (already done)
- [x] Snowflake schema (`MARKETLENS`) and base views
- [x] Signal SQL views (daily returns → volatility → anomaly → macro → summary)
- [x] Snowflake Python client with RSA key auth
- [x] Airflow standalone launcher
- [x] Streamlit app shell

---

### Phase 1 — Live Ingestion via Airflow (no Kafka yet)

**Goal:** Replace the heartbeat stub with a real daily DAG that fetches live prices and writes directly to Snowflake. Validates the end-to-end pipeline before adding Kafka complexity.

#### 1.1 Add a Snowflake staging table

In `setup.sql` (or a new `migrations/01_add_raw_tables.sql`):

```sql
CREATE TABLE IF NOT EXISTS RAW_STOCK_PRICES (
    TICKER        VARCHAR,
    DATE          DATE,
    OPEN_PRICE    FLOAT,
    HIGH_PRICE    FLOAT,
    LOW_PRICE     FLOAT,
    CLOSE_PRICE   FLOAT,
    VOLUME        BIGINT,
    SOURCE        VARCHAR,       -- 'yfinance', 'alpaca', etc.
    INGESTED_AT   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 1.2 Write a YFinance fetcher (`ingestion/yfinance_producer.py`)

Adapted from `daily_stock_analysis/data_provider/yfinance_fetcher.py`:

```
ingestion/
  __init__.py
  base_producer.py        # abstract: fetch_and_publish(tickers, date_range)
  yfinance_producer.py    # concrete: calls yfinance, writes rows to Snowflake
  macro_producer.py       # fetches FRED / Snowflake Free macro data
```

Key methods:
- `fetch_daily(tickers, start, end) -> list[UnifiedQuote]`
- `write_to_snowflake(quotes)` — bulk insert into `RAW_STOCK_PRICES`
- Retry with `tenacity` (3 attempts, exponential backoff) — same as `daily_stock_analysis`

#### 1.3 Replace the Airflow heartbeat DAG (`dags/marketlens_daily.py`)

```python
with DAG("marketlens_daily", schedule="0 18 * * 1-5",   # 6 PM weekdays
         start_date=datetime(2024, 1, 1), catchup=False,
         tags=["marketlens"]) as dag:

    ingest_prices = PythonOperator(
        task_id="ingest_prices",
        python_callable=run_yfinance_producer,   # fetches + writes to SF
    )
    refresh_signals = SnowflakeOperator(
        task_id="refresh_signals",
        sql="CALL MARKETLENS.REFRESH_SIGNALS();", # stored proc or individual SELECTs
        snowflake_conn_id="snowflake_marketlens",
    )
    anomaly_check = PythonOperator(
        task_id="anomaly_check",
        python_callable=query_and_log_anomalies,
    )

    ingest_prices >> refresh_signals >> anomaly_check
```

#### 1.4 Wire Airflow ↔ Snowflake connection

- Add `snowflake_marketlens` connection in Airflow UI (or via env var)
- Use `apache-airflow-providers-snowflake` provider package

**Deliverable:** A running daily DAG that populates `RAW_STOCK_PRICES`, refreshes signal views, and logs anomalies to Airflow task logs.

---

### Phase 2 — Add Kafka Between Ingestion and Snowflake

**Goal:** Decouple producers from Snowflake writes. Fetchers publish to Kafka; a separate consumer writes to Snowflake. This mirrors a real production data platform.

#### 2.1 Kafka setup

Run Kafka locally via Docker Compose:

```yaml
# docker-compose.kafka.yml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

Topics to create:
- `raw.stock.prices` — one message per ticker per day
- `raw.macro.indicators` — Fed rate, CPI updates

#### 2.2 Message schema (JSON envelope)

```json
{
  "ticker":      "AAPL",
  "date":        "2025-04-09",
  "open":        172.30,
  "high":        175.10,
  "low":         171.50,
  "close":       174.20,
  "volume":      58200000,
  "source":      "yfinance",
  "published_at": "2025-04-09T18:01:05Z"
}
```

#### 2.3 Refactor fetchers as Kafka producers (`ingestion/yfinance_producer.py`)

```python
from kafka import KafkaProducer
import json

class YFinanceProducer(BaseProducer):
    def fetch_and_publish(self, tickers, date):
        quotes = self._fetch(tickers, date)   # same yfinance logic as Phase 1
        for q in quotes:
            self.producer.send("raw.stock.prices", json.dumps(q).encode())
```

#### 2.4 Write a Kafka → Snowflake consumer (`ingestion/sf_consumer.py`)

```python
from kafka import KafkaConsumer

class SnowflakeConsumer:
    def run(self, topic="raw.stock.prices"):
        for msg in self.consumer:
            quote = json.loads(msg.value)
            self.snowflake_client.insert_raw_price(quote)
```

#### 2.5 Update Airflow DAG to include Kafka tasks

```
ingest_prices (publish to Kafka)
    │
    ▼
consume_to_snowflake (Kafka → Snowflake, run as sensor or timed task)
    │
    ▼
refresh_signals (SnowflakeOperator)
    │
    ▼
anomaly_check
```

**Deliverable:** Messages flow through Kafka. You can inspect topic lag in Kafka UI. Snowflake is only written to by the consumer, not directly by the fetcher.

---

### Phase 3 — Spark / Flink Stream Processing (optional, for class requirement)

**Goal:** Replace the direct Kafka → Snowflake consumer with a Spark Structured Streaming or Flink job that adds a transformation step before writing.

#### Option A: Spark Structured Streaming

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev

spark = SparkSession.builder.appName("MarketLens").getOrCreate()

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "raw.stock.prices")
      .load())

# Parse JSON, compute 5-day rolling average as early enrichment
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write enriched stream to Snowflake staging
parsed.writeStream.foreachBatch(write_to_snowflake).start()
```

Use case: compute a **5-minute VWAP** or **intraday rolling average** before data lands in Snowflake — something the static SQL views cannot do on streaming data.

#### Option B: Apache Flink (PyFlink)

Better suited if you want true low-latency event-time processing. The Flink job:
1. Reads `raw.stock.prices` from Kafka
2. Applies a tumbling 1-day window, computes OHLCV aggregates
3. Writes to a Snowflake Kafka connector sink or an intermediate staging table

#### Decision guidance

| If your class requires... | Use |
|---|---|
| Batch processing + SQL transforms | Spark (easier Snowflake integration) |
| True streaming + event-time windows | Flink |
| Neither required | Skip Phase 3; Snowflake views already handle all transforms |

---

### Phase 4 — Streamlit Dashboard

The dashboard reads from Snowflake views already defined in `signals/`.

#### Pages to build

| Page | Source view | Content |
|---|---|---|
| Signal Summary | `V_SIGNAL_SUMMARY` | Table of today's anomalies + macro events, sorted by salience score |
| Stock Deep Dive | `V_ANOMALY_SCORES`, `V_ROLLING_VOLATILITY` | Per-ticker z-score chart, 20-day volatility band |
| Macro Overlay | `V_FED_RATE_CHANGES`, `V_CPI_CHANGES` | Fed rate and CPI timeline with stock return overlay |
| Pipeline Health | Airflow REST API or task logs | DAG last run status, task durations |

---

## 6. Directory Structure (target)

```
MarketLens/
├── app/
│   ├── app.py                    # Streamlit entry point
│   └── snowflake_client.py       # existing Snowflake connection
├── dags/
│   ├── marketlens_heartbeat.py   # existing stub (replace with below)
│   └── marketlens_daily.py       # Phase 1+ real DAG
├── ingestion/
│   ├── __init__.py
│   ├── base_producer.py          # abstract fetcher/producer
│   ├── yfinance_producer.py      # Phase 1: write to SF; Phase 2: write to Kafka
│   ├── macro_producer.py         # FRED / Snowflake Free macro data
│   └── sf_consumer.py            # Phase 2: Kafka → Snowflake consumer
├── signals/
│   ├── 01_daily_returns.sql      # existing
│   ├── 02_rolling_volatility.sql # existing
│   ├── 03_anomaly_scores.sql     # existing
│   ├── 04_macro_signals.sql      # existing
│   └── 05_signal_summary.sql     # existing
├── migrations/
│   └── 01_add_raw_tables.sql     # Phase 1: RAW_STOCK_PRICES table
├── spark/                        # Phase 3 (optional)
│   └── stream_processor.py
├── docker-compose.kafka.yml      # Phase 2: Kafka + Zookeeper
├── setup.sql                     # existing schema bootstrap
├── start.sh                      # existing launcher
├── scripts/
│   └── airflow_standalone.sh     # existing
└── requirements.txt
```

---

## 7. Dependency additions (by phase)

| Phase | New packages |
|---|---|
| Phase 1 | `apache-airflow-providers-snowflake`, `yfinance`, `tenacity` |
| Phase 2 | `kafka-python` (or `confluent-kafka`), `docker-compose` |
| Phase 3 | `pyspark` or `apache-flink` (PyFlink) |

---

## 8. Milestone Summary

| Milestone | What works end-to-end |
|---|---|
| **M1** (Phase 1) | Airflow DAG runs daily, `RAW_STOCK_PRICES` populated, signal views refresh, anomalies logged |
| **M2** (Phase 2) | Prices flow through Kafka; consumer writes to Snowflake; Airflow orchestrates all steps |
| **M3** (Phase 3, optional) | Spark/Flink job sits between Kafka and Snowflake, adding a streaming transform step |
| **M4** (Phase 4) | Streamlit dashboard shows live signal summary, anomaly table, and macro overlay |
