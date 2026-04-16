# MarketLens — Implementation Plan

> **Status as of April 12, 2026**
> Phases 0, 1, and 4 are fully complete. Phase 2 is scaffolded and ready to activate.
> Phase 3 (Spark/Flink) remains optional and has not been started.
> This document is a record of how the system was designed and why — not a to-do list.
> For what to build next, see `roadmap.md`.

---

## 1. Project Overview

MarketLens is a **server-level financial data engineering platform** for US equity analysis. It ingests stock prices and macro indicators, computes trading signals inside Snowflake, and surfaces anomalies and summaries through a Streamlit dashboard with AI-powered explanations.

Unlike lightweight single-process tools (e.g. `daily_stock_analysis`), MarketLens separates concerns across dedicated infrastructure layers: **Airflow** for orchestration, **Snowflake** as the analytical warehouse, **Kafka** for event streaming (Phase 2), and optionally **Spark/Flink** for distributed stream processing (Phase 3).

---

## 2. State When Planning Began

This plan was written after the initial dashboard existed but before the ingestion layer, production DAG, or notification system were built.

| Component | Status at planning time |
|---|---|
| Snowflake schema + base views | Done |
| Signal SQL views (returns, vol, anomaly, macro) | Done |
| Snowflake Python client (RSA key auth) | Done |
| Streamlit dashboard (chat + market pulse) | Exists, scope TBD |
| Airflow DAG | Stub only (heartbeat ping task) |
| Airflow standalone launcher | Done |
| Kafka | Not started |
| Spark / Flink | Not started |
| Live data ingestion | Not started |

---

## 3. Target Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                  │
│                                                                   │
│  YFinanceProducer ─┐                                             │
│  MacroProducer ────┼──► Kafka Topic: raw.stock.prices    (Ph.2)  │
│                    └──► Kafka Topic: raw.macro.indicators (Ph.2)  │
│                         (Phase 1: write directly to Snowflake)   │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  PROCESSING LAYER                                                 │
│  Phase 1: direct write to Snowflake (DONE)                       │
│  Phase 2: SnowflakePricesConsumer reads Kafka → Snowflake        │
│  Phase 3: Spark/Flink sits here for streaming transforms         │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  STORAGE + ANALYTICS LAYER  (Snowflake, MARKETLENS schema)       │
│                                                                   │
│  RAW_STOCK_PRICES  ──► V_DAILY_RETURNS                           │
│                    ──► V_ROLLING_VOLATILITY                      │
│                    ──► V_ANOMALY_SCORES                          │
│  RAW_MACRO_INDICATORS ► V_FED_RATE_CHANGES, V_CPI_CHANGES        │
│                    ──► V_YIELD_CURVE, V_YIELD_CURVE_SIGNALS      │
│                    ──► V_SIGNAL_SUMMARY                          │
│  PIPELINE_RUN_LOG  ──► Pipeline Health dashboard                 │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION LAYER  (Airflow)  (DONE)                          │
│                                                                   │
│  marketlens_daily DAG  — weekdays 6 PM ET                        │
│  [ingest_prices, ingest_macro] >> refresh_signals                │
│                                >> anomaly_check >> notify        │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  NOTIFICATION LAYER  (DONE)                                       │
│  SlackSender · EmailSender · broadcast (fail-open)               │
└──────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  SERVING LAYER  (Streamlit)  (DONE)                              │
│  Chat · Stock Deep Dive · Macro Overlay · Pipeline Health        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Design Decisions Inspired by `daily_stock_analysis`

`daily_stock_analysis` solved the hardest part of ingestion — multi-source fallback, normalization, and retry — in its `data_provider/` layer. MarketLens borrowed these patterns and promoted them to a dedicated ingestion framework.

| Pattern from `daily_stock_analysis` | MarketLens adaptation |
|---|---|
| `BaseFetcher` abstract class | `BaseProducer` with circuit breaker + `fetch_with_guard()` |
| Priority-ordered fallback chain (efinance → akshare → yfinance) | `run_with_fallback(producers, tickers, date)` — tries producers in ascending priority order |
| `UnifiedRealtimeQuote` normalized struct | `UnifiedQuote` dataclass — source-agnostic contract for all price data |
| `tenacity` retry with exponential backoff | Same decorator on `YFinanceProducer._fetch_raw()` — 3 attempts, 2–30s backoff |
| Source tag on each record | `source` field on `UnifiedQuote` + `QUERY_ID` UUID for pipeline lineage tracing |

One addition beyond `daily_stock_analysis`: a **circuit breaker** on each producer. After `CIRCUIT_BREAKER_FAILURES` consecutive failures, the producer enters a cooldown period and raises `RuntimeError` immediately instead of attempting a slow failing call. This prevents a broken data source from dragging down the whole pipeline.

---

## 5. Phased Implementation Plan

### Phase 0 — Foundation ✅ Complete
*Pre-existed before this plan was written.*

- [x] Snowflake schema (`MARKETLENS`) and base views
- [x] Signal SQL views (daily returns → volatility → anomaly → macro → summary)
- [x] Snowflake Python client with RSA key-pair auth
- [x] Airflow standalone launcher (`scripts/airflow_standalone.sh`)
- [x] Streamlit app shell with chat + market pulse

---

### Phase 1 — Live Ingestion via Airflow ✅ Complete
*Implemented on branch `yuzihaowashu`, April 12, 2026.*

**Goal:** Replace the heartbeat stub with a real daily DAG that fetches live prices and macro data, writes directly to Snowflake, and sends anomaly alerts. Validates the full end-to-end pipeline before adding Kafka complexity.

#### What was built

**`ingestion/base_producer.py`**
- `UnifiedQuote` dataclass — normalized price record with auto-generated UUID `query_id` for lineage tracing
- `BaseProducer` abstract class — circuit breaker (`record_failure`, `record_success`, `is_available`), `fetch_with_guard()` that raises `RuntimeError` when in cooldown
- `run_with_fallback(producers, tickers, date)` — sorts producers by priority, tries each, raises `DataFetchError` if all fail

**`ingestion/yfinance_producer.py`**
- `YFinanceProducer(BaseProducer)` — calls `yf.download()`, normalizes MultiIndex DataFrame via `_normalize()`
- Tenacity retry: 3 attempts, exponential backoff (2–30 seconds)
- `fetch_and_write_to_snowflake(tickers, date, conn)` — idempotent `MERGE INTO RAW_STOCK_PRICES` on `(TICKER, DATE, SOURCE)`
- `fetch_and_publish_to_kafka(tickers, date, kafka_producer, topic)` — ready for Phase 2

**`ingestion/macro_producer.py`**
- Free tier: Fed Funds Rate, CPI from Snowflake Marketplace
- Paid tier (gated by `SNOWFLAKE_PAID_DATA_AVAILABLE`): 10Y Treasury yield, Unemployment Rate
- Fail-open: each indicator fetched independently; one failure never blocks others
- `fetch_and_write_to_snowflake(conn)` — `MERGE INTO RAW_MACRO_INDICATORS`

**`dags/marketlens_daily.py`**
- Schedule: `0 23 * * 1-5` (6 PM ET on weekdays)
- `catchup=False`, `retries=1`, tagged `marketlens`
- Task flow: `[ingest_prices, ingest_macro] >> refresh_signals >> anomaly_check >> notify`
- `_log_run()` helper: best-effort `MERGE INTO PIPELINE_RUN_LOG` (non-fatal on failure)
- `_anomaly_check()`: queries `V_SIGNAL_SUMMARY`, pushes rows to XCom
- `_notify()`: pulls XCom from `anomaly_check`, calls `broadcast([SlackSender(), EmailSender()], ...)`

**`notification/`**
- `BaseSender` — `safe_send()` catches all exceptions and returns `False`; `broadcast()` skips unconfigured channels
- `SlackSender` — POST JSON to webhook URL; returns `True` only on HTTP 200 + `ok` response
- `EmailSender` — SMTP with STARTTLS; sends plain-text + HTML parts; `is_configured()` requires to_addr + user + password

**`migrations/01_add_raw_tables.sql`**
- `RAW_STOCK_PRICES` — PK: `(TICKER, DATE, SOURCE)`
- `RAW_MACRO_INDICATORS` — PK: `(VARIABLE, GEO_ID, DATE)`
- `PIPELINE_RUN_LOG` — PK: `(RUN_ID, TASK_ID)`, records start/complete/fail events per task

**`signals/06_new_macro_signals.sql`**
- Formalizes `new-signals-branch` SQL into proper views
- `V_10Y_TREASURY`, `V_UNEMPLOYMENT_RATE` (paid data, gated)
- `V_YIELD_CURVE` — 10Y minus Fed Funds Rate, `IS_INVERTED` flag
- `V_YIELD_CURVE_SIGNALS` — inversion flip events using `QUALIFY ROW_NUMBER()`

---

### Phase 2 — Kafka Streaming Layer 🔧 Scaffolded, not yet activated

**Goal:** Decouple producers from Snowflake writes. Fetchers publish to Kafka; a separate consumer writes to Snowflake. This mirrors a real production data platform and makes the pipeline more resilient — the producer doesn't care if Snowflake is briefly down.

#### What is already built

**`ingestion/sf_consumer.py`** — `SnowflakePricesConsumer` reads from `raw.stock.prices`, bulk-merges into `RAW_STOCK_PRICES`, commits offsets after successful write. Raises a helpful `ImportError` if `kafka-python` is not installed.

**`ingestion/yfinance_producer.py`** — `fetch_and_publish_to_kafka()` is already implemented alongside the Phase 1 Snowflake path.

**`docker-compose.kafka.yml`** — Zookeeper + Kafka (port 9092) + Kafka UI (port 8080), with health checks and 7-day log retention.

#### Kafka message schema

```json
{
  "ticker":       "AAPL",
  "date":         "2025-04-09",
  "open":         172.30,
  "high":         175.10,
  "low":          171.50,
  "close":        174.20,
  "volume":       58200000,
  "source":       "yfinance",
  "query_id":     "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

#### To activate Phase 2

1. Uncomment `kafka-python>=2.0,<3` in `requirements.txt` and run `pip install kafka-python`
2. Start the Kafka stack: `docker compose -f docker-compose.kafka.yml up -d`
3. In `dags/marketlens_daily.py`, swap `fetch_and_write_to_snowflake` → `fetch_and_publish_to_kafka` in `_ingest_prices()`
4. Add `SnowflakePricesConsumer` as a service in `docker-compose.kafka.yml`

**Deliverable:** Prices flow through Kafka. Kafka UI shows topic lag. Snowflake is written to only by the consumer, not directly by the fetcher.

---

### Phase 3 — Spark / Flink Stream Processing ⏳ Not started (optional)

**Goal:** Replace the direct Kafka → Snowflake consumer with a Spark Structured Streaming or Flink job that adds a transformation step before writing. This is the right phase to do if the class requires hands-on Spark or Flink work.

**Important context:** Spark and Flink are *not* needed for performance at our current data volume (9 tickers, daily OHLCV). Snowflake SQL handles the signal computation in milliseconds. The right reason to implement this phase is to *learn* the tools and demonstrate distributed stream processing in a real pipeline context.

To use Flink meaningfully, a real-time data source is needed — Yahoo Finance has a 15-minute delay and doesn't stream. Prefer **lawful, contract-compliant** market data (licensed vendor, course sandbox, etc.). For hands-on practice without touching third-party gray areas, **replay historical rows or synthetic ticks from your own files into Kafka** and run Flink on that stream — the streaming mechanics are the same as with a live vendor feed.

#### Option A — Apache Spark Structured Streaming

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, stddev
from pyspark.sql.types import StructType, StringType, FloatType, LongType

spark = SparkSession.builder.appName("MarketLens").getOrCreate()

schema = StructType()  # define UnifiedQuote fields

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "raw.stock.prices")
      .load())

parsed = df.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# Example: compute 5-minute VWAP before writing to Snowflake
parsed.writeStream.foreachBatch(write_to_snowflake).start()
```

Good for: batch-style enrichment (VWAP, rolling averages) before data lands in Snowflake.

#### Option B — Apache Flink (PyFlink)

Better suited for true low-latency event-time processing with watermarks.

1. Read `raw.stock.prices` from Kafka
2. Apply a tumbling 1-day window, compute OHLCV aggregates
3. Emit intraday anomaly events to an `alerts` Kafka topic
4. Write to Snowflake via Kafka connector sink

| If your class requires... | Recommended tool |
|---|---|
| Batch processing + SQL transforms | Spark (easier Snowflake integration via `spark-snowflake`) |
| True streaming + event-time windows | Flink |
| Neither required | Skip Phase 3 — Snowflake views already handle all transforms |

---

### Phase 4 — Streamlit Dashboard ✅ Complete
*Implemented on branch `yuzihaowashu`, April 12, 2026.*

**Goal:** Extend the existing chat page with three new data-driven pages that visualize the signals and pipeline state produced by Phases 1–2.

| Page | Source | What it shows |
|---|---|---|
| **Chat** | Snowflake Cortex `llama3.1-70b` | AI-powered Q&A, adapts to Beginner / Intermediate / Analyst level |
| **Stock Deep Dive** | `V_ANOMALY_SCORES` | Z-score chart, rolling volatility, anomaly day table for any ticker |
| **Macro Overlay** | `V_FED_RATE_CHANGES`, `V_CPI_CHANGES`, `V_YIELD_CURVE` | Fed rate history, CPI month-over-month, yield curve with inversion markers |
| **Pipeline Health** | `PIPELINE_RUN_LOG` | Run counts, status breakdown, data freshness, full run log |

**Performance decisions made during Phase 4:**
- Snowflake connection cached with `st.cache_resource` — shared across all sessions, created once per app process
- Removed `SELECT 1` health-check round-trip before every query — replaced with `is_closed()` local check
- All `@st.cache_data` query functions moved to module level so caches persist across page switches and Streamlit reruns
- Largest remaining bottleneck is Snowflake warehouse cold start (15–30s after inactivity) — mitigated by setting warehouse auto-suspend to 10–15 min in the Snowflake UI

---

## 6. Final Directory Structure

```
MarketLens/
├── app/
│   ├── app.py                    # Streamlit — Chat, Deep Dive, Macro, Pipeline Health
│   └── snowflake_client.py       # Connection with st.cache_resource + warehouse fallback
├── dags/
│   ├── marketlens_daily.py       # Production DAG (Phase 1 complete)
│   └── marketlens_heartbeat.py   # Original stub (kept for reference)
├── ingestion/
│   ├── base_producer.py          # UnifiedQuote, BaseProducer, circuit breaker, run_with_fallback
│   ├── yfinance_producer.py      # Phase 1: Snowflake write; Phase 2: Kafka publish
│   ├── macro_producer.py         # Free + paid Snowflake Marketplace indicators
│   └── sf_consumer.py            # Phase 2: Kafka → Snowflake consumer (ready, not activated)
├── notification/
│   ├── base_sender.py            # BaseSender, broadcast, build_signal_table
│   ├── slack_sender.py           # Webhook-based Slack alerts
│   └── email_sender.py           # SMTP/STARTTLS email alerts
├── signals/
│   ├── 01_daily_returns.sql
│   ├── 02_rolling_volatility.sql
│   ├── 03_anomaly_scores.sql
│   ├── 04_macro_signals.sql
│   ├── 05_signal_summary.sql
│   └── 06_new_macro_signals.sql  # Yield curve + inversion signals (from new-signals-branch)
├── migrations/
│   └── 01_add_raw_tables.sql     # RAW_STOCK_PRICES, RAW_MACRO_INDICATORS, PIPELINE_RUN_LOG
├── tests/
│   ├── test_config_parsing.py
│   ├── test_base_producer.py
│   ├── test_yfinance_producer.py
│   ├── test_notification.py
│   └── test_dag_import.py        # AST-based DAG structure tests (no Airflow runtime needed)
├── docker-compose.kafka.yml      # Phase 2: Kafka + Zookeeper + Kafka UI
├── setup.sql
├── config.py
├── requirements.txt
├── start.sh
├── plan.md                       # This file — design record
├── roadmap.md                    # Forward-looking ideas and directions for the team
└── progress.md                   # Version history and what was learned
```

---

## 7. Dependencies by Phase

| Phase | Packages added |
|---|---|
| Phase 0 | `streamlit`, `snowflake-connector-python`, `apache-airflow`, `python-dotenv` |
| Phase 1 | `yfinance>=0.2`, `tenacity>=8.2`, `requests>=2.31` |
| Phase 2 | `kafka-python>=2.0` (uncomment in `requirements.txt`) |
| Phase 3 | `pyspark` or `apache-flink` (PyFlink) |

---

## 8. Milestone Summary

| Milestone | Status | What works end-to-end |
|---|---|---|
| **M0** Phase 0 | ✅ Done | Snowflake schema, signal views, Streamlit shell, Airflow launcher |
| **M1** Phase 1 | ✅ Done | Daily DAG populates `RAW_STOCK_PRICES` + `RAW_MACRO_INDICATORS`, refreshes signals, detects anomalies, sends Slack/email alerts, logs to `PIPELINE_RUN_LOG` |
| **M2** Phase 2 | 🔧 Scaffolded | Code exists; needs `kafka-python` installed and DAG switched to publish mode |
| **M3** Phase 3 | ⏳ Not started | Spark/Flink job between Kafka and Snowflake; requires real-time data source for Flink |
| **M4** Phase 4 | ✅ Done | Dashboard shows live signals, anomaly charts, macro overlay, and pipeline run history |
