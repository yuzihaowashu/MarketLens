#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# MarketLens — one-click start script
#
# Usage:
#   ./start.sh              Launch the Streamlit app
#   ./start.sh --setup      Run SQL setup files first, then launch
#   ./start.sh --setup-only Run SQL setup files only (don't launch app)
#   ./start.sh --kill       Stop any running MarketLens instance
# ─────────────────────────────────────────────────────────────────────
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${MARKETLENS_PORT:-8501}"
VENV_DIR=".venv"
KEY_PATH="/Users/andrewhaggstrom/Desktop/CS Projects/Keys/rsa_key.p8"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYAN}[MarketLens]${NC} $*"; }
ok()    { echo -e "${GREEN}[MarketLens]${NC} $*"; }
err()   { echo -e "${RED}[MarketLens]${NC} $*" >&2; }

# ── Handle --kill ────────────────────────────────────────────────────
if [[ "$1" == "--kill" ]]; then
    info "Stopping any running MarketLens instances..."
    pkill -f "streamlit run.*app.py" 2>/dev/null && ok "Stopped." || info "No running instance found."
    exit 0
fi

# ── Check Python ─────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    err "python3 not found. Please install Python 3.9+."
    exit 1
fi

# ── Create / activate venv ───────────────────────────────────────────
if [[ ! -d "$VENV_DIR" ]]; then
    info "Creating virtual environment in ${VENV_DIR}..."
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

# ── Install dependencies ─────────────────────────────────────────────
if ! python -c "import streamlit, airflow" 2>/dev/null; then
    info "Installing Python dependencies..."
    pip install --quiet --upgrade pip
    pip install --quiet -r requirements.txt
    ok "Dependencies installed."
else
    info "Dependencies already installed."
fi

# ── Check RSA key ────────────────────────────────────────────────────
if [[ ! -f "$KEY_PATH" ]]; then
    err "Snowflake RSA private key not found at: $KEY_PATH"
    err "Set SNOWFLAKE_PRIVATE_KEY env var or copy your key there."
    exit 1
fi

# ── Suppress Streamlit email prompt ──────────────────────────────────
mkdir -p ~/.streamlit
if [[ ! -f ~/.streamlit/credentials.toml ]]; then
    cat > ~/.streamlit/credentials.toml <<'EOF'
[general]
email = ""
EOF
    info "Created ~/.streamlit/credentials.toml (suppresses email prompt)."
fi

# ── SQL setup (optional) ─────────────────────────────────────────────
run_sql_setup() {
    info "Running Snowflake SQL setup..."

    SQL_FILES=(
        "setup.sql"
        "migrations/02_add_fred_table.sql"
        "migrations/03_add_sec_tables.sql"
        "signals/01_daily_returns.sql"
        "signals/02_rolling_volatility.sql"
        "signals/03_anomaly_scores.sql"
        "signals/04_macro_signals.sql"
        "signals/07_sec_narratives.sql"
        "signals/05_signal_summary.sql"
    )

    python3 - "$KEY_PATH" "${SQL_FILES[@]}" <<'PYEOF'
import sys, os

key_path = sys.argv[1]
os.environ['SNOWFLAKE_PRIVATE_KEY'] = key_path

sys.path.insert(0, 'app')
sys.path.insert(0, '.')
from snowflake_client import get_connection

sql_files = sys.argv[2:]

conn = get_connection(force_new=True)
cursor = conn.cursor()
try:
    for fpath in sql_files:
        print(f"  Executing {fpath} ...")
        with open(fpath) as f:
            content = f.read()
        for stmt in content.split(';'):
            stmt = stmt.strip()
            lines = [l for l in stmt.splitlines() if not l.strip().startswith('--')]
            clean = '\n'.join(lines).strip()
            if clean:
                cursor.execute(clean)
    print("  SQL setup complete.")
finally:
    cursor.close()
    conn.close()
PYEOF

    ok "All SQL files executed successfully."
}

if [[ "$1" == "--setup" || "$1" == "--setup-only" ]]; then
    run_sql_setup
    if [[ "$1" == "--setup-only" ]]; then
        ok "Setup complete. Exiting (--setup-only)."
        exit 0
    fi
fi

# ── Kill existing instance on same port ──────────────────────────────
if lsof -i :"$PORT" &>/dev/null; then
    info "Port $PORT is in use. Stopping existing process..."
    pkill -f "streamlit run.*app.py" 2>/dev/null || true
    sleep 2
fi

# ── Launch Streamlit ─────────────────────────────────────────────────
info "Starting MarketLens on port ${BOLD}${PORT}${NC}..."
echo ""
echo -e "  ${GREEN}${BOLD}Open in browser:${NC}  http://localhost:${PORT}"
echo ""

streamlit run app/app.py \
    --server.port "$PORT" \
    --server.headless true
