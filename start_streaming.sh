#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# MarketLens — Phase 2 Kafka streaming layer
#
# Prerequisites:
#   1. docker compose -f docker-compose.kafka.yml up -d
#   2. pip install "kafka-python>=2.0,<3"    (or uncomment in requirements.txt)
#
# Usage:
#   ./start_streaming.sh          Start all four streaming services
#   ./start_streaming.sh --kill   Stop any running streaming services
#
# Kafka UI (see topics & consumer lag live):
#   http://localhost:8080
# ─────────────────────────────────────────────────────────────────────
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

info() { echo -e "${CYAN}[Streaming]${NC} $*"; }
ok()   { echo -e "${GREEN}[Streaming]${NC} $*"; }
err()  { echo -e "${RED}[Streaming]${NC} $*" >&2; }

# ── Kill mode ─────────────────────────────────────────────────────────
if [[ "$1" == "--kill" ]]; then
    info "Stopping streaming services..."
    pkill -f "ingestion.tick_producer"      2>/dev/null && ok "tick_producer stopped"      || true
    pkill -f "ingestion.anomaly_consumer"   2>/dev/null && ok "anomaly_consumer stopped"   || true
    pkill -f "ingestion.notifier_consumer"  2>/dev/null && ok "notifier_consumer stopped"  || true
    pkill -f "ingestion.dashboard_consumer" 2>/dev/null && ok "dashboard_consumer stopped" || true
    exit 0
fi

# ── Activate venv ─────────────────────────────────────────────────────
if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
fi

# ── Check kafka-python ────────────────────────────────────────────────
if ! python -c "import kafka" 2>/dev/null; then
    err "kafka-python not installed. Run: pip install 'kafka-python>=2.0,<3'"
    exit 1
fi

# ── Check Kafka broker reachable ──────────────────────────────────────
if ! python -c "
import socket, sys
try:
    s = socket.create_connection(('localhost', 9092), timeout=3)
    s.close()
except Exception:
    sys.exit(1)
" 2>/dev/null; then
    err "Kafka not reachable at localhost:9092"
    err "Start it with: docker compose -f docker-compose.kafka.yml up -d"
    exit 1
fi

pids=()

cleanup() {
    echo ""
    info "Stopping streaming services (Ctrl-C)..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    exit 0
}
trap cleanup INT TERM

# ── Launch services ───────────────────────────────────────────────────
info "Starting tick producer (GBM simulated prices → Kafka)..."
python -m ingestion.tick_producer > logs/tick_producer.log 2>&1 &
pids+=($!)

sleep 1

info "Starting anomaly consumer (Z-score detector → signals.anomalies)..."
python -m ingestion.anomaly_consumer > logs/anomaly_consumer.log 2>&1 &
pids+=($!)

info "Starting notifier consumer (signals.anomalies → Slack/email)..."
python -m ingestion.notifier_consumer > logs/notifier_consumer.log 2>&1 &
pids+=($!)

info "Starting dashboard consumer (prices → live_feed.json)..."
python -m ingestion.dashboard_consumer > logs/dashboard_consumer.log 2>&1 &
pids+=($!)

mkdir -p logs

echo ""
echo -e "  ${GREEN}${BOLD}Streaming services running:${NC}"
echo -e "  Tick producer      PID ${pids[0]}   → logs/tick_producer.log"
echo -e "  Anomaly consumer   PID ${pids[1]}   → logs/anomaly_consumer.log"
echo -e "  Notifier consumer  PID ${pids[2]}   → logs/notifier_consumer.log"
echo -e "  Dashboard consumer PID ${pids[3]}   → logs/dashboard_consumer.log"
echo ""
echo -e "  ${CYAN}Kafka UI:${NC}  http://localhost:8080"
echo -e "  ${CYAN}Dashboard:${NC} http://localhost:8501  (Live Feed page)"
echo ""
echo -e "  Press ${BOLD}Ctrl-C${NC} to stop all services."

wait
