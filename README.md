# MarketLens

**Understanding market signals for non-expert users.**

MarketLens is a financial data engineering pipeline and interactive dashboard built with Airflow, Snowflake, and Streamlit. It ingests live stock prices and macro-economic indicators, computes anomaly signals, and delivers AI-powered explanations — all tailored to the user's experience level (Beginner / Intermediate / Analyst).

**Apple Silicon (Mac M-series)?** See **[docs/MAC_SILICON_SETUP.md](docs/MAC_SILICON_SETUP.md)** for a step-by-step environment walkthrough.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Orchestration Layer (Apache Airflow 2.x)                    │
│  DAG: marketlens_daily  — weekdays at 6 PM ET                │
│  [ingest_prices, ingest_macro] >> refresh_signals            │
│                               >> anomaly_check >> notify     │
├─────────────────────────┬────────────────────────────────────┤
│  Ingestion Layer        │  Phase 2: Kafka (optional)         │
│  • YFinanceProducer     │  • docker-compose.kafka.yml        │
│    – circuit breaker    │  • Topic: raw.stock.prices         │
│    – tenacity retries   │  • Topic: raw.macro.indicators     │
│    – MERGE INTO SF      │  • SnowflakePricesConsumer         │
│  • MacroProducer        │                                    │
│    – free + paid tiers  │                                    │
│    – fail-open          │                                    │
├─────────────────────────┴────────────────────────────────────┤
│  Data Warehouse (Snowflake)                                  │
│  RAW_STOCK_PRICES · RAW_MACRO_INDICATORS · PIPELINE_RUN_LOG  │
│  Signal models (dbt, materialized as V_* views):            │
│    staging → price marts → macro marts → signal_summary     │
├──────────────────────────────────────────────────────────────┤
│  Notification Layer                                          │
│  SlackSender · EmailSender · broadcast (fail-open)          │
├──────────────────────────────────────────────────────────────┤
│  Dashboard (Streamlit)                                       │
│  Chat · Stock Deep Dive · Macro Overlay · Pipeline Health   │
│  LLM: Snowflake Cortex llama3.1-70b                         │
└──────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | 3.9 or later (3.11–3.12 recommended if you hit pandas build issues) |
| **Snowflake account** | With access to [Snowflake Public Data Products (Free)](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255/) |
| **Snowflake RSA key pair** | Private key in PEM (PKCS#8) format, unencrypted |
| **Snowflake Cortex** | Account must have Cortex LLM functions enabled (for AI chat) |
| **Docker** *(Phase 2 only)* | Required only if you want to run Kafka locally |
| **Apache Airflow 2.x** *(optional)* | For scheduled pipeline runs; installed from `requirements.txt` |

---

## Step-by-Step Setup

### Step 1 — Clone the repository

```bash
git clone <repo-url>
cd MarketLens
```

---

### Step 2 — Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

Or use the one-click script (creates the venv and installs deps automatically):

```bash
chmod +x start.sh
./start.sh
```

---

### Step 3 — Configure environment variables

> **Heads-up for forks:** `config.py` ships with machine-specific defaults for
> `SNOWFLAKE_USER` (`GRIZZLY`) and `SNOWFLAKE_PRIVATE_KEY_PATH`
> (`/Users/andrewhaggstrom/Desktop/CS Projects/Keys/rsa_key.p8`) — these are
> the maintainer's local values. **Do not edit `config.py`.** Override both in
> your shell or `.env`:
> ```bash
> export SNOWFLAKE_USER="YOUR_SF_USER"
> export SNOWFLAKE_PRIVATE_KEY_PATH="/absolute/path/to/your/rsa_key.p8"
> ```

---

Copy the template and fill in your values:

```bash
cp .env.example .env
```

Open `.env` in any editor and set at minimum:

| Variable | What to set |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Your account locator, e.g. `XY12345-AB67890` |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_DATABASE` | Database where MarketLens lives (e.g. `SCORPION_DB`) |
| `SNOWFLAKE_SCHEMA` | Schema name — default `MARKETLENS` |
| `SNOWFLAKE_WAREHOUSE` | Warehouse your role can USE (run `SHOW WAREHOUSES` if unsure) |
| `SNOWFLAKE_ROLE` | Role with access to the DB, warehouse, and Marketplace data |
| `SNOWFLAKE_PRIVATE_KEY` | Absolute path to your `.p8` RSA private key, e.g. `~/airflow/snowflake_rsa_key.p8` |

**Optional — notifications:**

| Variable | Purpose |
|---|---|
| `SLACK_WEBHOOK_URL` | Incoming webhook URL for Slack alerts |
| `ALERT_EMAIL` | Recipient address for email alerts |
| `SMTP_USER` / `SMTP_PASSWORD` | Gmail (or other SMTP) credentials |
| `SMTP_HOST` / `SMTP_PORT` | Default: `smtp.gmail.com` / `587` |

**Optional — ingestion tuning:**

| Variable | Default | Purpose |
|---|---|---|
| `FETCH_RETRY_ATTEMPTS` | `3` | Tenacity retries per producer call |
| `CIRCUIT_BREAKER_FAILURES` | `3` | Failures before a producer enters cooldown |
| `CIRCUIT_BREAKER_COOLDOWN` | `300` | Cooldown duration in seconds |
| `PIPELINE_LOG_ENABLED` | `true` | Write run events to `PIPELINE_RUN_LOG` |
| `SNOWFLAKE_PAID_DATA_AVAILABLE` | `false` | Set `true` to enable 10Y Treasury + Unemployment views |

> **Secrets stay on your machine.** `.env` and your `.p8` key are both listed in `.gitignore`. Never commit them.

---

### Step 4 — Add the free Snowflake Marketplace data

In the Snowflake web console:

1. Go to **Marketplace** → search **"Snowflake Public Data Products — Free"**
2. Click **Get** — the shared database appears as `SNOWFLAKE_PUBLIC_DATA_FREE`

This provides stock prices (Cybersyn) and macro indicators (Fed, BLS) at no cost.

---

### Step 5 — Run the one-time SQL setup

Run the following files **in order** in a Snowflake worksheet (Snowsight) or via SnowSQL:

```
setup.sql                          -- creates MARKETLENS schema + base views
signals/01_daily_returns.sql       -- daily return %
signals/02_rolling_volatility.sql  -- 20-day rolling volatility
signals/03_anomaly_scores.sql      -- Z-score anomaly detection
signals/04_macro_signals.sql       -- Fed rate + CPI change views
signals/05_signal_summary.sql      -- unified signal summary
signals/06_new_macro_signals.sql   -- yield curve + inversion signals (paid data, optional)
```

Then run the **ingestion layer migration** to create the raw tables and pipeline log:

```
migrations/01_add_raw_tables.sql   -- RAW_STOCK_PRICES, RAW_MACRO_INDICATORS, PIPELINE_RUN_LOG
```

> **Tip:** `./start.sh --setup` runs these automatically via the Python connector if your `.env` is configured.

---

### Step 6 — Launch the Streamlit dashboard

```bash
source .venv/bin/activate
streamlit run app/app.py --server.port 8501
```

Open **http://localhost:8501** in your browser.

The dashboard has four pages (use the sidebar to navigate):

| Page | What it shows |
|---|---|
| **Chat** | AI-powered Q&A about your portfolio using Snowflake Cortex |
| **Stock Deep Dive** | Z-score anomaly chart + volatility overlay for any ticker |
| **Macro Overlay** | Fed Funds Rate, CPI, and Yield Curve tabs |
| **Pipeline Health** | Run log, data freshness, and ingestion row counts |

---

### Step 7 — Run the pipeline manually (optional)

To ingest today's data without waiting for the Airflow schedule, run directly:

```bash
source .venv/bin/activate
python - <<'EOF'
import sys; sys.path.insert(0, '.')
import config as cfg
from ingestion.yfinance_producer import YFinanceProducer
from ingestion.macro_producer import MacroProducer
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
import snowflake.connector, datetime

with open(cfg.SNOWFLAKE_PRIVATE_KEY_PATH, 'rb') as f:
    pk = load_pem_private_key(f.read(), password=None).private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
conn = snowflake.connector.connect(
    account=cfg.SNOWFLAKE_ACCOUNT, user=cfg.SNOWFLAKE_USER, private_key=pk,
    database=cfg.SNOWFLAKE_DATABASE, schema=cfg.SNOWFLAKE_SCHEMA,
    warehouse=cfg.SNOWFLAKE_WAREHOUSE, role=cfg.SNOWFLAKE_ROLE,
)
today = datetime.date.today()
n = YFinanceProducer().fetch_and_write_to_snowflake(cfg.WATCHLIST_TICKERS, today, conn)
print(f"Stock rows written: {n}")
MacroProducer().fetch_and_write_to_snowflake(conn)
print("Macro indicators written.")
conn.close()
EOF
```

---

### Step 8 — Set up Airflow for scheduled runs (optional)

The DAG `marketlens_daily` runs the full pipeline every weekday at 6 PM ET. To run it locally:

```bash
chmod +x scripts/airflow_standalone.sh
./scripts/airflow_standalone.sh
```

- **Airflow UI:** http://localhost:8080 — log in as `admin`
- **Password:** stored in `.airflow/standalone_admin_password.txt` (created automatically; gitignored)
- **DAGs folder:** `dags/` — Airflow will detect `marketlens_daily.py` automatically
- **Enable the DAG:** toggle it on in the Airflow UI, or trigger a manual run with the play button

The DAG task flow:

```
[ingest_prices, ingest_macro]   ← run in parallel
          ↓
    refresh_signals             ← validates all 7 signal views
          ↓
    anomaly_check               ← queries V_SIGNAL_SUMMARY, stores rows in XCom
          ↓
       notify                   ← broadcasts to Slack + Email (if configured)
```

If `SLACK_WEBHOOK_URL` or `ALERT_EMAIL` / SMTP credentials are not set, the notify task still succeeds — unconfigured channels are silently skipped.

---

### Step 9 — Enable Kafka streaming (Phase 2, optional)

Kafka is scaffolded but disabled by default. To activate it:

**1. Start the Kafka stack:**

```bash
docker compose -f docker-compose.kafka.yml up -d
```

- Kafka broker: `localhost:9092`
- Kafka UI: http://localhost:8080

**2. Install the Kafka client:**

Uncomment `kafka-python>=2.0,<3` in `requirements.txt`, then:

```bash
pip install kafka-python
```

**3. Switch the DAG to publish mode:**

In `dags/marketlens_daily.py`, replace `fetch_and_write_to_snowflake` with `fetch_and_publish_to_kafka` in `_ingest_prices()`, then run `SnowflakePricesConsumer` as a separate process to consume from Kafka and bulk-merge into Snowflake.

---

## Running the Tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

All 69 tests run without a live Snowflake connection (Snowflake calls are mocked).

| Test file | What it covers |
|---|---|
| `tests/test_config_parsing.py` | `parse_env_bool`, `parse_env_int`, `parse_env_list` |
| `tests/test_base_producer.py` | `UnifiedQuote`, circuit breaker, `run_with_fallback` |
| `tests/test_yfinance_producer.py` | normalization, Snowflake write, Kafka publish |
| `tests/test_notification.py` | `broadcast`, `SlackSender`, `EmailSender` |
| `tests/test_dag_import.py` | DAG structure (AST-based, no Airflow runtime needed) |
| `tests/test_dbt_project.py` | dbt project structure: aliases, docs, ref/source graph integrity |

---

## dbt (signals layer)

The signals layer is a dbt project at `dbt/`. Models materialize as views and
keep the legacy `V_*` names via `{{ config(alias='V_...') }}`, so every app/DAG
query works unchanged.

```bash
# One-time: copy the template and export env vars (config.py reads the same ones)
cp dbt/profiles.example.yml dbt/profiles.yml

# Build every model + run every test
cd dbt && dbt deps --profiles-dir . && dbt build --profiles-dir .

# Exclude paid-marketplace models
dbt build --profiles-dir . --exclude stg_10y_treasury+ stg_unemployment+

# Generate the lineage graph
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

The DAG runs `dbt build --select +signal_summary+` in its `refresh_signals`
task. A failing dbt test aborts the DAG before the notification step ever sees
stale data.

Legacy `signals/*.sql` files remain on disk as a rollback path but are no
longer executed by `start.sh --setup`.

---

## Project Structure

```
MarketLens/
├── start.sh                        # One-click venv + launch
├── scripts/
│   └── airflow_standalone.sh       # Local Airflow 2.x in standalone mode
├── requirements.txt
├── .env.example                    # Template — copy to .env and fill in
├── config.py                       # All settings; loads .env via python-dotenv
│
├── migrations/
│   └── 01_add_raw_tables.sql       # RAW_STOCK_PRICES, RAW_MACRO_INDICATORS, PIPELINE_RUN_LOG
│
├── ingestion/
│   ├── base_producer.py            # UnifiedQuote, BaseProducer, circuit breaker, run_with_fallback
│   ├── yfinance_producer.py        # YFinanceProducer — yfinance + tenacity retries
│   ├── macro_producer.py           # MacroProducer — Fed rate, CPI (free), Treasury, Unemployment (paid)
│   └── sf_consumer.py              # Kafka → Snowflake consumer (Phase 2)
│
├── notification/
│   ├── base_sender.py              # BaseSender, broadcast, build_signal_table
│   ├── slack_sender.py             # SlackSender (webhook)
│   └── email_sender.py             # EmailSender (SMTP/STARTTLS)
│
├── dags/
│   ├── marketlens_daily.py         # Main production DAG (weekdays 6 PM ET)
│   └── marketlens_heartbeat.py     # Minimal heartbeat DAG (example)
│
├── signals/
│   ├── 01_daily_returns.sql
│   ├── 02_rolling_volatility.sql
│   ├── 03_anomaly_scores.sql
│   ├── 04_macro_signals.sql
│   ├── 05_signal_summary.sql
│   └── 06_new_macro_signals.sql    # Yield curve + inversion signals (paid data)
│
├── app/
│   ├── app.py                      # Streamlit app (Chat, Deep Dive, Macro, Pipeline Health)
│   └── snowflake_client.py         # Snowflake connection helper
│
├── setup.sql                       # Schema + base views (run once)
├── docker-compose.kafka.yml        # Kafka + Zookeeper + Kafka UI (Phase 2)
├── plan.md                         # Full implementation plan
├── tests/
│   ├── test_config_parsing.py
│   ├── test_base_producer.py
│   ├── test_yfinance_producer.py
│   ├── test_notification.py
│   └── test_dag_import.py
└── docs/
    └── MAC_SILICON_SETUP.md
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `Object does not exist` on a signal view | Run all `signals/*.sql` and `migrations/01_add_raw_tables.sql` in Snowflake |
| `PIPELINE_RUN_LOG does not exist` | Run `migrations/01_add_raw_tables.sql` |
| `Private key` errors | Ensure your `.p8` is unencrypted PKCS#8 PEM. Check `SNOWFLAKE_PRIVATE_KEY` in `.env` |
| `No active warehouse` (57P03) | Set `SNOWFLAKE_WAREHOUSE` to a warehouse your role can USE |
| `002043` / warehouse "does not exist" | Fix `SNOWFLAKE_WAREHOUSE` (run `SHOW WAREHOUSES` in Snowflake) |
| Paid data views return empty | Set `SNOWFLAKE_PAID_DATA_AVAILABLE=true` and subscribe to the paid Marketplace listing |
| Streamlit asks for email | `start.sh` creates `~/.streamlit/credentials.toml` with empty email when missing |
| LLM call fails | Verify Cortex is enabled: run `SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'hello')` in Snowsight |
| Port 8501 in use | `pkill -f "streamlit run"` or use `--server.port 8502` |
| Airflow "Address already in use" (8080/8794) | Stop duplicate Airflow processes; remove stale `.airflow/*.pid`; see `docs/MAC_SILICON_SETUP.md` |
| `ModuleNotFoundError: kafka` | Uncomment `kafka-python` in `requirements.txt` and run `pip install kafka-python` |
| Tests fail with `airflow.sdk` error | Tests use AST parsing — do not import airflow in test files; see `tests/test_dag_import.py` |

---

## Secrets Stay on Your Machine

| Secret | Where it lives | In git? |
|---|---|---|
| Snowflake credentials | `.env` (copied from `.env.example`) | No — `.gitignore` excludes `.env` |
| Snowflake private key (`.p8`) | Path in `SNOWFLAKE_PRIVATE_KEY` | No — never commit key files |
| Airflow admin password | `.airflow/standalone_admin_password.txt` | No — `.airflow/` is gitignored |
| Slack webhook URL | `SLACK_WEBHOOK_URL` in `.env` | No |
| SMTP password | `SMTP_PASSWORD` in `.env` | No |

Only `.env.example` (empty template) and `config.py` (non-secret placeholders) are committed to git.
