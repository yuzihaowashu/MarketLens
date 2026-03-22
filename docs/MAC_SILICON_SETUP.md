# MarketLens on Mac (Apple Silicon)

This guide walks through configuring a **native arm64** Python environment on **Apple Silicon** (M1 / M2 / M3 / M4) for MarketLens: Streamlit, Snowflake, optional **Apache Airflow 2.x**, and the RSA key layout used by the project.

> **Teammate checklist (any OS):** see **[README.md → New to this repository?](../README.md#new-to-this-repository)** for run order, and **[Local-only files you must keep and configure](../README.md#local-only-files-you-must-keep-and-configure)** for what stays on disk (`.env`, `.p8` key, optional `.airflow/`) and how each maps to **your Snowflake / local Airflow** settings.

---

## 1. System prerequisites

1. **macOS** with Apple Silicon (arm64). You do **not** need Rosetta for the Python stack described here; wheels from PyPI are built for `macosx_11_0_arm64`.
2. **Xcode Command Line Tools** (recommended). Some packages compile extensions if a wheel is missing:
   ```bash
   xcode-select --install
   ```
3. **Python 3.9+**. Install via [python.org](https://www.python.org/downloads/macos/) or Homebrew:
   ```bash
   brew install python@3.12
   ```
   Then use the exact interpreter you want when creating the venv, for example:
   ```bash
   /opt/homebrew/bin/python3.12 --version
   ```

### Python version note (especially 3.14+)

On **very new** Python releases, `pip` may try to build **pandas** from source and fail. This repo pins **`pandas==2.3.3`** in `requirements.txt` so a **binary wheel** is used when available. If installs still fail, use **Python 3.11 or 3.12** for the virtual environment.

---

## 2. Clone the repo and create a virtual environment

```bash
cd /path/to/your/work
git clone <repo-url> MarketLens
cd MarketLens

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Confirm imports:

```bash
python -c "import streamlit, airflow; print('streamlit + airflow OK')"
```

---

## 3. Snowflake RSA key (default path)

The app default (and `start.sh`) expects an **unencrypted PKCS#8** private key at:

```text
~/airflow/snowflake_rsa_key.p8
```

On Mac:

```bash
mkdir -p ~/airflow
# copy your key file here, or symlink:
# ln -s /path/to/your/key.p8 ~/airflow/snowflake_rsa_key.p8
```

You can override the path with **`SNOWFLAKE_PRIVATE_KEY`** in `.env` (supports `${HOME}/...`; expanded at runtime).

---

## 4. Environment file (`.env`)

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake account identifiers. Values are loaded automatically when Python imports **`config.py`** (`python-dotenv`). Shell exports still win if the same variable is already set.

Important keys:

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, … | Connection |
| `SNOWFLAKE_WAREHOUSE` | Primary warehouse |
| `SNOWFLAKE_WAREHOUSE_FALLBACKS` | Comma-separated alternates if the primary name does not exist for your role (`002043`) |
| `SNOWFLAKE_PRIVATE_KEY` | Path to `.p8` key |

Connection behavior is implemented in **`app/snowflake_client.py`**: after connect, the session runs **`USE ROLE` / `USE WAREHOUSE` / `USE DATABASE` / `USE SCHEMA`**, trying fallback warehouses when needed.

---

## 5. Run the Streamlit app

```bash
chmod +x start.sh
./start.sh
```

Open **http://localhost:8501**.

`start.sh` creates `.venv` if missing, installs dependencies if `streamlit` or `airflow` are not importable, ensures `~/.streamlit/credentials.toml` exists (suppresses the email prompt), then starts Streamlit.

Optional one-time SQL against Snowflake:

```bash
./start.sh --setup
```

---

## 6. Optional: Apache Airflow 2.x (local UI)

The project pins **Airflow 2.x** (`apache-airflow>=2.8,<3`), not Airflow 3.

```bash
chmod +x scripts/airflow_standalone.sh
./scripts/airflow_standalone.sh
```

- **`AIRFLOW_HOME`** defaults to **`<repo>/.airflow`** (gitignored).
- **DAGs** folder: **`dags/`** (includes `marketlens_heartbeat` as a smoke DAG).
- Web UI: **http://localhost:8080**
- Default **`admin`** password: **`<repo>/.airflow/standalone_admin_password.txt`**

### If Airflow fails with “Address already in use” (e.g. port 8794)

Usually **two** Airflow stacks are running (old `standalone` / `triggerer` plus a new one). Stop everything that uses this project’s venv, remove stale PID files, then restart:

```bash
pkill -f "MarketLens/.venv/bin/airflow"   # adjust path if your clone lives elsewhere
rm -f .airflow/airflow-webserver.pid .airflow/airflow-scheduler.pid
./scripts/airflow_standalone.sh
```

---

## 7. Quick reference

| Service | URL |
|---------|-----|
| MarketLens (Streamlit) | http://localhost:8501 |
| Airflow Web UI | http://localhost:8080 |

For general project behavior, SQL setup, and architecture, see **[README.md](../README.md)**.
