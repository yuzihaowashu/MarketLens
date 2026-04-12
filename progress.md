# MarketLens — Project Progress Log

A plain-English record of what was built in each version, why it matters, and what was learned.

---

## Version 1 — Initial Dashboard
**Branch:** `main` | **Date:** March 1–22, 2026 | **Commits:** `cffb0f8` → `fbe0b15`

### What we built
A working Streamlit web app that lets users chat about the stock market using AI. The app connects to Snowflake, reads market data from Snowflake's free Marketplace, and answers questions in plain English using Snowflake Cortex (a built-in LLM).

### Key files introduced
| File | Purpose |
|---|---|
| `app/app.py` | Main Streamlit UI — chat interface, market pulse cards, price charts |
| `app/snowflake_client.py` | Handles the Snowflake database connection using RSA key-pair login |
| `config.py` | Central settings file — loads credentials from `.env` |
| `setup.sql` | One-time SQL to create the `MARKETLENS` schema in Snowflake |
| `signals/01–05_*.sql` | Five SQL views that compute daily returns, rolling volatility, Z-score anomalies, Fed rate changes, CPI changes, and a unified signal summary |
| `requirements.txt` | Python dependencies (Streamlit, Snowflake connector, Airflow, dotenv) |
| `start.sh` | One-click script to set up the environment and launch the app |
| `.env.example` | Template so teammates can fill in their own Snowflake credentials safely |
| `dags/marketlens_heartbeat.py` | A minimal "hello world" Airflow DAG — proof that Airflow is set up and working |
| `docs/MAC_SILICON_SETUP.md` | Step-by-step guide for running everything on Apple Silicon Macs |

### What we learned
- How to connect a Python app to Snowflake using key-pair authentication
- How to use **Snowflake Marketplace** to get free financial data (stock prices, Fed rate, CPI) without building a scraper
- How to write **SQL views** for signal computation (instead of running the math in Python every time)
- How **Snowflake Cortex** can answer natural-language questions about data using a built-in LLM
- What an **Airflow DAG** looks like at its simplest — just a heartbeat task to verify the setup works
- How to use a **user experience level** (Beginner / Intermediate / Analyst) to change how the AI responds

### Architecture at this stage
```
User → Streamlit App → Snowflake (Marketplace data + Cortex LLM)
                            ↑
                       SQL Views (signals)
```

---

## Version 2 — New Macro Signals
**Branch:** `new-signals-branch` → merged into `main` | **Date:** April 12, 2026 | **Commit:** `268479e` → `b2e2a29`

### What we built
A teammate added new SQL queries to pull richer macro-economic data from Snowflake's Marketplace — specifically data from the Federal Reserve and the Bureau of Labor Statistics. These were merged into the main branch.

### Key files introduced
| File | Purpose |
|---|---|
| `signals/NewSignals.sql` | Raw SQL experiments: Fed Funds Rate from Federal Reserve, 10-Year Treasury yield, unemployment rate from BLS, and a yield curve inversion signal |

### What the new signals mean
| Signal | What it tells you |
|---|---|
| **Fed Funds Rate** | The interest rate the Federal Reserve sets — a major driver of stock market direction |
| **10-Year Treasury Yield** | Long-term government bond rate — used to measure investor confidence |
| **Unemployment Rate** | Percentage of people without jobs — a key health indicator for the economy |
| **Yield Curve Inversion** | When short-term rates are higher than long-term rates — historically a warning sign before recessions |

### What we learned
- How to query **Federal Reserve and BLS data** directly from Snowflake Marketplace (no API keys needed)
- What a **yield curve inversion** is and why it matters to investors
- How to use `QUALIFY ROW_NUMBER()` in SQL to detect the moment a signal flips (e.g., the exact day the yield curve inverts)
- How to manage **feature branches** in git and merge new work cleanly into main

### Architecture at this stage
```
User → Streamlit App → Snowflake (Marketplace data + Cortex LLM)
                            ↑
                  SQL Views (returns, volatility, Z-scores,
                             Fed rate, CPI, Treasury, unemployment,
                             yield curve inversion)
```

---

## Version 3 — Full Data Engineering Pipeline
**Branch:** `yuzihaowashu` | **Date:** April 12, 2026 | **Commit:** `348b778`

### What we built
A production-grade data pipeline on top of the existing dashboard. The app went from being a read-only viewer of Marketplace data to a system that **actively ingests, stores, processes, and alerts** on financial data. This version adds the classic components of a real data engineering stack: a pipeline orchestrator (Airflow), a streaming layer (Kafka), a multi-source ingestion framework, a notification system, and operational monitoring.

### New concepts introduced

#### Apache Airflow — Orchestration
Airflow is a tool that schedules and monitors workflows. We replaced the heartbeat stub with a real production DAG (`marketlens_daily`) that runs every weekday at 6 PM ET. It automatically fetches stock data, updates signals, checks for anomalies, and sends alerts — no human intervention needed.

```
[ingest_prices, ingest_macro]   ← two tasks run at the same time
          ↓
    refresh_signals             ← validates all 7 signal views in Snowflake
          ↓
    anomaly_check               ← finds unusual market movements
          ↓
       notify                   ← sends Slack + Email alerts
```

#### Apache Kafka — Streaming (Phase 2, optional)
Kafka is a message queue for high-volume, real-time data. We scaffolded the full Kafka setup but left it off by default. In Phase 1 (current), the pipeline writes directly to Snowflake. In Phase 2, producers publish to Kafka topics and a consumer reads from Kafka to write into Snowflake — making the system more resilient and scalable.

#### Multi-Source Ingestion with Fallback
Instead of depending on a single data source, we built a framework where multiple data providers can be tried in priority order. If the first source fails, the system automatically tries the next one. Each source also has a **circuit breaker** — if it fails too many times, it is paused temporarily so it doesn't slow everything down.

#### Idempotent Writes — MERGE INTO
All data is written using `MERGE INTO` instead of `INSERT`. This means if the same data is written twice (e.g., after a pipeline retry), Snowflake will update the existing row instead of creating a duplicate. This is critical for production reliability.

#### Notifications
When anomalies are detected, the pipeline can send alerts to Slack and/or email. If one channel fails (e.g., the Slack webhook is down), the other still sends — this is called **fail-open** design.

### Key files introduced
| File | What it does |
|---|---|
| `ingestion/base_producer.py` | Defines `UnifiedQuote` (a standard data format for all price data), `BaseProducer` (circuit breaker, retry guard), and `run_with_fallback` (tries sources in priority order) |
| `ingestion/yfinance_producer.py` | Downloads stock prices from Yahoo Finance (free, no API key). Retries automatically on failure. Writes to Snowflake or publishes to Kafka |
| `ingestion/macro_producer.py` | Fetches macro indicators from Snowflake Marketplace. Free tier: Fed Funds Rate, CPI. Paid tier (optional): 10Y Treasury, Unemployment Rate |
| `ingestion/sf_consumer.py` | Phase 2: reads stock price messages from Kafka and bulk-writes them into Snowflake |
| `notification/base_sender.py` | Base class for all alert channels. `broadcast()` sends to all configured channels and reports which ones succeeded |
| `notification/slack_sender.py` | Sends alerts to a Slack channel via webhook URL |
| `notification/email_sender.py` | Sends alerts via email (Gmail or any SMTP server) |
| `dags/marketlens_daily.py` | The production Airflow DAG — orchestrates all of the above on a schedule |
| `migrations/01_add_raw_tables.sql` | Creates 3 new Snowflake tables: `RAW_STOCK_PRICES`, `RAW_MACRO_INDICATORS`, `PIPELINE_RUN_LOG` |
| `signals/06_new_macro_signals.sql` | Formalizes the new-signals-branch SQL into proper views: yield curve, Treasury, unemployment, inversion detection |
| `docker-compose.kafka.yml` | Starts Kafka, Zookeeper, and Kafka UI with one command (`docker compose up -d`) |
| `plan.md` | Full written implementation plan with phases, design decisions, and rationale |

### New dashboard pages
| Page | What it shows |
|---|---|
| **Stock Deep Dive** | Pick any ticker and time period; see Z-score anomaly chart and volatility overlay pulled live from Snowflake |
| **Macro Overlay** | Three tabs: Fed Funds Rate history, CPI changes, and Yield Curve with inversion markers |
| **Pipeline Health** | Shows how many pipeline runs completed vs. failed, when data was last refreshed, and the full run log |

### Tests — 69 total, all passing
Every major component has unit tests that run without a live Snowflake connection (all database calls are mocked).

| Test file | What it checks |
|---|---|
| `test_config_parsing.py` | Config helper functions for reading environment variables |
| `test_base_producer.py` | Circuit breaker behavior, fallback chain ordering |
| `test_yfinance_producer.py` | Data normalization, Snowflake write logic, Kafka publish |
| `test_notification.py` | Broadcast behavior, Slack + Email send/fail scenarios |
| `test_dag_import.py` | DAG structure (uses Python AST parsing — avoids Airflow runtime issues on Python 3.14) |

### What we learned
- How **Airflow** orchestrates a real multi-step pipeline with parallelism and XCom (passing data between tasks)
- How **Kafka** fits into a data pipeline and why it improves resilience vs. writing directly to a database
- The **circuit breaker pattern** — pausing a failing service temporarily so it doesn't drag everything down
- **Idempotency** — designing writes so they are safe to replay without creating duplicates
- **Fail-open** notification design — one broken alert channel never blocks the others
- How to write **unit tests for data pipelines** without needing a real database
- How to work around Python version compatibility issues (Airflow SDK missing on Python 3.14) using AST-based testing

### Architecture at this stage
```
                    Apache Airflow (scheduler)
                           ↓
         ┌─────────────────────────────────┐
         ↓                                 ↓
  YFinanceProducer                   MacroProducer
  (Yahoo Finance)                    (Snowflake Marketplace)
         │                                 │
         │    Phase 2: via Kafka           │
         │    ┌──────────────┐             │
         └───→│  Kafka Topic │             │
              └──────┬───────┘             │
                     ↓                     │
              SnowflakePricesConsumer       │
                     │                     │
                     └────────┬────────────┘
                              ↓
                   Snowflake (RAW tables)
                              ↓
                      SQL Signal Views
                              ↓
              ┌───────────────┼───────────────┐
              ↓               ↓               ↓
          Streamlit       Anomaly Check    Pipeline Log
          Dashboard       → Slack/Email    Dashboard
```
