"""Optional reporting / extension metrics (non-critical path).

Teammates can implement functions in ``reports.extra_metrics`` without blocking
the daily DAG or core Streamlit flows. Wire-ins to the app or Airflow are
explicitly optional and should be reviewed by the core team.
"""

from reports.extra_metrics import (
    MetricRow,
    fred_macro_spread_metrics,
    liquidity_proxy_from_volumes,
    term_structure_kink_signal,
    watchlist_breadth_from_daily_returns,
)

__all__ = [
    'MetricRow',
    'fred_macro_spread_metrics',
    'liquidity_proxy_from_volumes',
    'term_structure_kink_signal',
    'watchlist_breadth_from_daily_returns',
]
