#!/usr/bin/env bash
# Local Airflow 2.x (scheduler + webserver + one admin user) for MarketLens.
# First run prints login credentials. DAGs: ../dags
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

VENV="${ROOT}/.venv"
if [[ ! -d "$VENV" ]]; then
  echo "No .venv found. Run ./start.sh once (or: python3 -m venv .venv && pip install -r requirements.txt)" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "${VENV}/bin/activate"

export AIRFLOW_HOME="${AIRFLOW_HOME:-${ROOT}/.airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-${ROOT}/dags}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-False}"

mkdir -p "$AIRFLOW_HOME"

if ! python -c "import airflow" 2>/dev/null; then
  echo "apache-airflow not installed. Run: pip install -r requirements.txt" >&2
  exit 1
fi

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DAGs: $AIRFLOW__CORE__DAGS_FOLDER"
echo "Starting Airflow standalone (2.x). First run may take a minute and will print UI login."
exec airflow standalone
