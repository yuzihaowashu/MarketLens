# MarketLens

**Understanding market signals for non-expert users.**

MarketLens is an interactive financial market assistant built with Streamlit and Snowflake. It lets users explore stock prices, macro-economic signals, and AI-powered explanations — all tailored to the user's experience level (beginner / intermediate / analyst).

**Apple Silicon (Mac M-series)?** See **[docs/MAC_SILICON_SETUP.md](docs/MAC_SILICON_SETUP.md)** for a step-by-step environment walkthrough.

---

## New to this repository?

Follow this order once per machine. **Nothing secret from your machine should be committed to git** (see [Secrets stay on your machine](#secrets-stay-on-your-machine) below).

| Step | Action |
|------|--------|
| 1 | **Clone** the repo and `cd` into the project folder. |
| 2 | **Install dependencies:** run `./start.sh` (creates `.venv` and installs packages) **or** manually: `python3 -m venv .venv`, `source .venv/bin/activate`, `pip install -r requirements.txt`. |
| 3 | **Create local config:** `cp .env.example .env` and **fill every row in [Configuration you must provide](#configuration-you-must-provide)** that applies to your Snowflake user. |
| 4 | **Private key:** put your Snowflake **RSA private key** (`.p8`, unencrypted PKCS#8) at the path in `SNOWFLAKE_PRIVATE_KEY`, or update that variable. Default expectation: `~/airflow/snowflake_rsa_key.p8`. |
| 5 | **Snowflake data:** in the Snowflake web UI, add the free Marketplace listing *Snowflake Public Data Products — Free* (`SNOWFLAKE_PUBLIC_DATA_FREE`). |
| 6 | **One-time SQL:** run `setup.sql` then `signals/*.sql` in order, **or** run `./start.sh --setup` (uses your `.env` / `config.py` connection). |
| 7 | **Run the app:** `./start.sh` → open **http://localhost:8501**. |
| 8 | *(Optional)* **Airflow 2.x:** `chmod +x scripts/airflow_standalone.sh && ./scripts/airflow_standalone.sh` → **http://localhost:8080**. Log in as **`admin`**; the **generated password** is stored **only on your computer** in **`.airflow/standalone_admin_password.txt`** (inside the hidden `.airflow` folder). |

If anything fails, check [Troubleshooting](#troubleshooting) and the Mac guide in **`docs/MAC_SILICON_SETUP.md`**.

### Local-only files you must keep and configure

These paths live **on each teammate’s computer**. They are **not** in git (or should never be added). **Configure them using your own Snowflake account details** (and optional local Airflow). **Do not commit** them to the team repository.

| Path | How you get it | What you configure (your account / machine) |
|------|----------------|-----------------------------------------------|
| **`.env`** | Copy from **`.env.example`**: `cp .env.example .env` | **Snowflake:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, path to your key in `SNOWFLAKE_PRIVATE_KEY`, and optionally `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE_FALLBACKS`. Optional Airflow-related keys if you override defaults. |
| **RSA private key file** (e.g. **`.p8`**) | You (or your instructor) generate a Snowflake key pair; keep the **private** key on disk only | **Snowflake key-pair auth:** unencrypted PKCS#8 PEM. Put the file where `SNOWFLAKE_PRIVATE_KEY` in `.env` points (default in examples: **`~/airflow/snowflake_rsa_key.p8`**). This file is as sensitive as a password. |
| **`.venv/`** | Created by `python3 -m venv .venv` or by **`./start.sh`** | **Not** Snowflake-specific—your local Python packages. Safe to delete and recreate; never commit. |
| **`.airflow/`** *(optional)* | Created when you run **`./scripts/airflow_standalone.sh`** (or `airflow` with `AIRFLOW_HOME` pointing here) | **Local Airflow only:** metadata DB (`airflow.db`), generated **`airflow.cfg`**, **`standalone_admin_password.txt`** (UI password for user `admin`), logs, PID files. Nothing here is shared via git; each machine has its own. |

**Checked into git (templates only):** **`.env.example`** — safe to commit; it has **no secrets** and **no real account values**. Everyone copies it to **`.env`** and fills in their own.

**Optional (not Snowflake):** `start.sh` may create **`~/.streamlit/credentials.toml`** on your Mac/Linux profile to skip Streamlit’s email prompt—that file stays in your **home** directory, not in the repo.

### Configuration you must provide

These values must match **your** Snowflake account (or your course’s shared naming scheme). They go in **`.env`** (recommended) or can be exported in the shell. **`config.py` has example defaults**, but your account almost certainly needs different values—**do not assume the defaults work for you.**

| Variable | You must set? | What it is |
|----------|----------------|------------|
| `SNOWFLAKE_ACCOUNT` | **Yes** (usually) | Account locator, e.g. `XY12345-AB67890`. |
| `SNOWFLAKE_USER` | **Yes** (usually) | Your Snowflake username. |
| `SNOWFLAKE_DATABASE` | **Yes** (usually) | Database where your app schema lives. |
| `SNOWFLAKE_ROLE` | **Yes** (usually) | Role with access to that database, warehouse, and Marketplace data. |
| `SNOWFLAKE_WAREHOUSE` | **Yes** (usually) | A warehouse your role can `USE` (run `SHOW WAREHOUSES` in Snowflake if unsure). |
| `SNOWFLAKE_PRIVATE_KEY` | **Yes** (path) | Absolute or `~` path to your **unencrypted** `.p8` private key file. |
| `SNOWFLAKE_SCHEMA` | Often default is fine | Default `MARKETLENS` unless your project uses another schema. |
| `SNOWFLAKE_WAREHOUSE_FALLBACKS` | Optional | Comma-separated backup warehouse names if the primary name errors with “does not exist” (`002043`). |

**Snowflake authentication:** this app uses **key-pair login**, not a Snowflake password in `.env`. Your **private key file** is sensitive—treat it like a password and **never commit it**.

### Secrets stay on your machine

This is the same information as **[Local-only files you must keep and configure](#local-only-files-you-must-keep-and-configure)**, focused on **secrets**:

| Secret / config | Where it lives | In git? |
|-----------------|----------------|---------|
| Snowflake connection values | **`.env`** (created from **`.env.example`**, filled with **your** account) | **No** — `.gitignore` excludes `.env`. |
| Snowflake **private key** (`.p8`) | File path set in **`SNOWFLAKE_PRIVATE_KEY`** (often under **`~/`** ) | **No** — never commit key files. |
| Airflow UI **`admin` password** | **`.airflow/standalone_admin_password.txt`** (after running local Airflow) | **No** — **`.airflow/`** is gitignored. On macOS, `.airflow` is a **hidden folder** (name starts with a dot); in Finder use **Go → Go to Folder…** → `<your-clone-path>/.airflow/`. |

There is **no** shared team password or Snowflake key stored in this Git repository—only **`.env.example`** (empty template) and **`config.py`** (non-secret placeholders).

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | 3.9 or later (3.11–3.12 recommended if you hit pandas build issues on bleeding-edge Python) |
| **Snowflake account** | With access to [Snowflake Public Data Products (Free)](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255/) |
| **Snowflake RSA key pair** | Private key in PEM (PKCS#8) format, unencrypted |
| **Snowflake Cortex** | The account must have Cortex LLM functions enabled (for AI explanations) |
| **Optional: Apache Airflow** | This repo installs **Airflow 2.x** only (`>=2.8`, `<3`) for local orchestration experiments—not Airflow 3 |

Python dependencies live in **`requirements.txt`**: Streamlit, Snowflake connector, **python-dotenv**, pinned **pandas==2.3.3** (avoids broken source builds on some very new Python versions), **apache-airflow** 2.x, and **apache-airflow-providers-snowflake**.

## Quick Start

The fastest way to get running is the included `start.sh` script:

```bash
cd MarketLens
chmod +x start.sh
./start.sh
```

This will create a virtual environment, install dependencies, and launch the app. See below for manual setup if you prefer step-by-step.

---

## Manual Setup

### 1. Clone / copy the project

```bash
git clone <repo-url>
cd MarketLens
```

### 2. Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure Snowflake connection

**Recommended:** copy **`.env.example`** to **`.env`** and fill in your account, user, database, warehouse, role, and private key path. On import, **`config.py`** loads `.env` via **`python-dotenv`**. Variables already exported in your shell are **not** overridden (useful for CI or Airflow).

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE` | Connection defaults |
| `SNOWFLAKE_PRIVATE_KEY` | Path to RSA `.p8` (supports `${HOME}/...`) |
| `SNOWFLAKE_WAREHOUSE_FALLBACKS` | Optional comma-separated warehouse names tried if `USE WAREHOUSE` fails with “does not exist” (`002043`) |

You can still rely on the built-in defaults in **`config.py`** if you prefer not to use a `.env` file.

Snowflake access is implemented in **`app/snowflake_client.py`**: it connects with key-pair auth, then runs **`USE ROLE`**, **`USE WAREHOUSE`** (with fallbacks), **`USE DATABASE`**, and **`USE SCHEMA`** so the session always has an active warehouse when your role allows it.

### 4. Get Snowflake Marketplace data

In the Snowflake web console:
1. Go to **Marketplace** → search for **"Snowflake Public Data Products — Free"**
2. Click **Get** to add it to your account (zero-copy, no cost)
3. The shared database will appear as `SNOWFLAKE_PUBLIC_DATA_FREE`

### 5. Run the SQL setup (one-time)

Open a Snowflake worksheet (or use SnowSQL / any SQL client) and execute the SQL files **in order**:

```
setup.sql                        -- creates MARKETLENS schema + base views
signals/01_daily_returns.sql     -- daily return view
signals/02_rolling_volatility.sql -- 20-day rolling volatility view
signals/03_anomaly_scores.sql    -- Z-score anomaly detection view
signals/04_macro_signals.sql     -- Fed rate + CPI change views
signals/05_signal_summary.sql    -- unified signal summary view
```

> **Tip:** `start.sh --setup` will execute these automatically via the Snowflake Python connector.

### 6. Launch the app

```bash
source .venv/bin/activate
streamlit run app/app.py --server.port 8501 --server.headless true
```

Open **http://localhost:8501** in your browser.

### 7. Optional: local Airflow 2.x UI

For a bundled scheduler + webserver + triggerer (development only):

```bash
chmod +x scripts/airflow_standalone.sh
./scripts/airflow_standalone.sh
```

- **Web UI:** http://localhost:8080  
- **DAGs:** `dags/` (see `marketlens_heartbeat` as a minimal example)  
- **Metadata / config:** `.airflow/` (created automatically; gitignored)  
- **Admin password file:** `.airflow/standalone_admin_password.txt`  

If you see **“Address already in use”** on the triggerer port (often **8794**), you likely have two Airflow processes; stop all `airflow` invocations using this project’s `.venv`, remove stale `.airflow/*.pid` files if needed, then start again. Details: **[docs/MAC_SILICON_SETUP.md](docs/MAC_SILICON_SETUP.md)**.

---

## Project Structure

```
MarketLens/
├── start.sh                     # One-click launch (Streamlit)
├── scripts/
│   └── airflow_standalone.sh    # Local Airflow 2.x (standalone mode)
├── requirements.txt             # Streamlit + Snowflake + Airflow 2.x + dotenv
├── .env.example                 # Template for Snowflake / optional Airflow env
├── config.py                    # Defaults; loads .env when present
├── setup.sql                    # Schema + base views over Marketplace data
├── dags/
│   └── marketlens_heartbeat.py  # Example DAG for Airflow
├── signals/
│   ├── 01_daily_returns.sql
│   ├── 02_rolling_volatility.sql
│   ├── 03_anomaly_scores.sql
│   ├── 04_macro_signals.sql
│   └── 05_signal_summary.sql
├── app/
│   ├── app.py                   # Streamlit main application
│   └── snowflake_client.py      # Snowflake connection + session setup
├── docs/
│   └── MAC_SILICON_SETUP.md     # Apple Silicon environment guide
└── README.md
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│  User Interface (Streamlit)                     │
│  • Level selection (Beginner / Intermediate /   │
│    Analyst)                                     │
│  • Chat with streaming AI responses             │
│  • Market Pulse, Price Charts, Signal Cards     │
├─────────────────────────────────────────────────┤
│  LLM Layer (Snowflake Cortex)                   │
│  • llama3.1-70b via SNOWFLAKE.CORTEX.COMPLETE   │
│  • System prompt adapts to user's level         │
├─────────────────────────────────────────────────┤
│  Signal Layer (SQL Views)                       │
│  • Daily returns, rolling volatility, Z-scores  │
│  • Fed Funds Rate changes, CPI changes          │
│  • Unified signal summary with salience scores  │
├─────────────────────────────────────────────────┤
│  Data Layer (Snowflake Marketplace)             │
│  • Stock prices (Cybersyn)                      │
│  • Macro indicators (Fed, BLS)                  │
│  • SEC filings text                             │
│  • Zero-copy data sharing — no ETL needed       │
└─────────────────────────────────────────────────┘
```

## Adding New Signals

1. Create a new `.sql` file in `signals/` (e.g. `06_my_signal.sql`)
2. Define a `CREATE OR REPLACE VIEW V_MY_SIGNAL AS ...` query
3. Add a `UNION ALL` block in `signals/05_signal_summary.sql` to include it
4. Re-run the updated SQL files in Snowflake
5. The new signal will automatically appear in the app

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SNOWFLAKE_PRIVATE_KEY` | `~/airflow/snowflake_rsa_key.p8` | Path to your RSA private key file |
| `SNOWFLAKE_*` | See `config.py` / `.env.example` | Account, user, database, schema, warehouse, role |
| `SNOWFLAKE_WAREHOUSE_FALLBACKS` | `SCORPION_WH` | Comma-separated alternates if primary warehouse is missing for your role |
| `AIRFLOW_HOME` | (set by `airflow_standalone.sh`) | Defaults to `<repo>/.airflow` when using the bundled script |
| `AIRFLOW__CORE__DAGS_FOLDER` | `<repo>/dags` | Set by `airflow_standalone.sh` unless overridden |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` | Recommended in `.env.example` |

## Troubleshooting

| Problem | Solution |
|---|---|
| `Object does not exist` errors | Make sure you ran all SQL files in order, and that the Marketplace database `SNOWFLAKE_PUBLIC_DATA_FREE` is accessible |
| `Private key` errors | Ensure your `.p8` file is unencrypted PEM PKCS#8 format. Check `SNOWFLAKE_PRIVATE_KEY` in `.env` or the default path |
| `No active warehouse` (57P03) | Set `SNOWFLAKE_WAREHOUSE` to a warehouse your role can use; the app runs `USE WAREHOUSE` after connect |
| `002043` / warehouse “does not exist” | Primary warehouse name may be wrong; set `SNOWFLAKE_WAREHOUSE_FALLBACKS` or fix `SNOWFLAKE_WAREHOUSE` (run `SHOW WAREHOUSES` in Snowflake) |
| Streamlit asks for email | `start.sh` creates `~/.streamlit/credentials.toml` with empty email when missing |
| LLM call fails | Verify your Snowflake account has Cortex enabled. Try running `SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'hello')` in a worksheet |
| Port 8501 already in use | `./start.sh --kill` or `pkill -f "streamlit run"` or use `--server.port 8502` |
| Airflow “Address already in use” (8794 / 8080) | Stop duplicate Airflow processes; see **docs/MAC_SILICON_SETUP.md** |
| `pip` fails building pandas | Use Python 3.11 or 3.12, or keep `pandas==2.3.3` from `requirements.txt` |
