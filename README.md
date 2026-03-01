# MarketLens

**Understanding market signals for non-expert users.**

MarketLens is an interactive financial market assistant built with Streamlit and Snowflake. It lets users explore stock prices, macro-economic signals, and AI-powered explanations — all tailored to the user's experience level (beginner / intermediate / analyst).

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | 3.9 or later |
| **Snowflake account** | With access to [Snowflake Public Data Products (Free)](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255/) |
| **Snowflake RSA key pair** | Private key in PEM (PKCS#8) format, unencrypted |
| **Snowflake Cortex** | The account must have Cortex LLM functions enabled (for AI explanations) |

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
pip install -r requirements.txt
```

### 3. Configure Snowflake connection

Edit **two files** to match your own Snowflake account:

**`app/snowflake_client.py`** — update the connection parameters inside `get_connection()`:

```python
return snowflake.connector.connect(
    account='YOUR_ACCOUNT',        # e.g. 'SFEDU02-UNB02139'
    user='YOUR_USERNAME',           # e.g. 'SCORPION'
    private_key=_load_private_key(key_path),
    database='YOUR_DATABASE',       # e.g. 'SCORPION_DB'
    schema='MARKETLENS',
    warehouse='YOUR_WAREHOUSE',     # e.g. 'SCORPION_WH'
    role='YOUR_ROLE',               # e.g. 'TRAINING_ROLE'
)
```

**`config.py`** — update `SNOWFLAKE_CONN` and marketplace references if your database names differ.

You also need to tell the app where your RSA private key is. Either:
- Set the environment variable: `export SNOWFLAKE_PRIVATE_KEY=/path/to/your/key.p8`
- Or edit the default path in `app/snowflake_client.py`

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

---

## Project Structure

```
MarketLens/
├── start.sh                  # One-click launch script
├── requirements.txt          # Python dependencies
├── config.py                 # Snowflake connection constants & parameters
├── setup.sql                 # Schema + base views over Marketplace data
├── signals/
│   ├── 01_daily_returns.sql       # Daily return calculation
│   ├── 02_rolling_volatility.sql  # 20-day rolling volatility
│   ├── 03_anomaly_scores.sql      # Z-score anomaly detection
│   ├── 04_macro_signals.sql       # Fed rate + CPI change detection
│   └── 05_signal_summary.sql      # Union all signals into one view
├── app/
│   ├── app.py                     # Streamlit main application
│   └── snowflake_client.py        # Snowflake connection helper
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

## Troubleshooting

| Problem | Solution |
|---|---|
| `Object does not exist` errors | Make sure you ran all SQL files in order, and that the Marketplace database `SNOWFLAKE_PUBLIC_DATA_FREE` is accessible |
| `Private key` errors | Ensure your `.p8` file is unencrypted PEM PKCS#8 format. Check the path in env var or `snowflake_client.py` |
| Streamlit asks for email | Create `~/.streamlit/credentials.toml` with `[general]` and `email = ""` |
| LLM call fails | Verify your Snowflake account has Cortex enabled. Try running `SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'hello')` in a worksheet |
| Port 8501 already in use | Kill existing processes: `pkill -f "streamlit run"` or use `--server.port 8502` |
