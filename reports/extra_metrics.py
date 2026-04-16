"""
Extension metrics — optional, team-assigned.

These functions consume *already-fetched* Python structures (dicts / lists).
``app/app.py`` loads the latest watchlist / FRED / volume snapshots from
Snowflake, calls these functions, and **renders a Streamlit section only when
at least one function returns a non-empty** ``list[MetricRow]``. Teammates
implement logic here only — **do not edit the Streamlit frontend** for this
feature.

Assigned teammate: implement the bodies marked ``TODO``. Keep return types
stable (``list[MetricRow]``) so callers can iterate safely. Empty list means
“nothing to show yet” or “not implemented”.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence


@dataclass(frozen=True)
class MetricRow:
    """One human-facing metric line for UI, Slack digests, or logs."""

    metric_id: str
    """Stable snake_case id, e.g. ``breadth_up``."""

    label: str
    """Short title shown to users."""

    value: float | str | None
    """Primary number or text; None if not computable."""

    interpretation: str
    """One sentence: what this means for the watchlist or macro picture."""


# ---------------------------------------------------------------------------
# 1) Cross-sectional equity — uses existing daily return data
# ---------------------------------------------------------------------------

def watchlist_breadth_from_daily_returns(
    ticker_to_daily_return_pct: Mapping[str, float],
) -> list[MetricRow]:
    """
    Use **existing** same-day percentage returns for each ticker (e.g. from
    ``daily_returns`` / ``anomaly_scores`` in Snowflake, already exposed in
    the Streamlit app as a dict ``{ \"AAPL\": 0.42, ... }`` meaning +0.42%).

    Suggested implementations (pick one or more rows):

    - **breadth_up**: fraction of tickers with return > 0.
    - **median_return**: median of cross-sectional returns.
    - **dispersion**: max(return) − min(return) as a simple stress proxy.

    Parameters
    ----------
    ticker_to_daily_return_pct
        Keys are upper-case tickers; values are *percent* changes for one day
        (e.g. ``1.25`` means +1.25%, not 0.0125).

    Returns
    -------
    list[MetricRow]
        Empty until implemented. When done, return one ``MetricRow`` per
        sub-metric you want to expose.
    """
    # TODO(assigned teammate): implement using ticker_to_daily_return_pct
    _ = ticker_to_daily_return_pct
    return []


# ---------------------------------------------------------------------------
# 2) Macro — uses latest FRED-aligned scalars already in the pipeline
# ---------------------------------------------------------------------------

def fred_macro_spread_metrics(
    latest_by_variable: Mapping[str, float],
) -> list[MetricRow]:
    """
    Use **latest** values keyed by the same *logical* names used in
    ``RAW_FRED_INDICATORS`` / dbt staging (examples — align with your DB)::

        TREASURY_10Y, INFLATION_EXPECTATIONS_10Y, CONSUMER_SENTIMENT, ...

    Ideas:

    - **real_yield_proxy**: ``TREASURY_10Y - INFLATION_EXPECTATIONS_10Y`` if
      both exist (rough narrative only — document limitations).
    - **sentiment_vs_median**: compare ``CONSUMER_SENTIMENT`` to a rolling
      median if you add a second input later; for v1, static thresholds only.

    Parameters
    ----------
    latest_by_variable
        Map variable name → latest observed *level* (already as float).

    Returns
    -------
    list[MetricRow]
        Empty until implemented.
    """
    # TODO(assigned teammate): derive 1–3 MetricRow objects from latest_by_variable
    _ = latest_by_variable
    return []


# ---------------------------------------------------------------------------
# 3) Liquidity / activity — uses volume vs a baseline (existing raw data)
# ---------------------------------------------------------------------------

def liquidity_proxy_from_volumes(
    ticker_to_volume: Mapping[str, float],
    ticker_to_avg_volume_20d: Mapping[str, float],
) -> list[MetricRow]:
    """
    Compare **today’s** volume to a **20-trading-day average** per ticker
    (both already landable from Yahoo / ``RAW_STOCK_PRICES`` aggregates).

    Ideas:

    - **volume_z**: (vol − avg) / std if you extend signature with std;
      v1 can use simple ratio ``vol / avg`` clipped to [0.5, 5.0].
    - **watchlist_avg_ratio**: mean of per-ticker ratios as a single headline.

    Parameters
    ----------
    ticker_to_volume
        Today’s volume per ticker.
    ticker_to_avg_volume_20d
        Average daily volume over the last 20 sessions (same units).

    Returns
    -------
    list[MetricRow]
        Empty until implemented.
    """
    # TODO(assigned teammate): implement ratio or z-style metrics
    _ = ticker_to_volume, ticker_to_avg_volume_20d
    return []


# ---------------------------------------------------------------------------
# 4) Optional — time series slice (if you add new FRED pulls elsewhere)
# ---------------------------------------------------------------------------

def term_structure_kink_signal(
    dates_and_3m: Sequence[tuple[date, float]],
    dates_and_10y: Sequence[tuple[date, float]],
) -> list[MetricRow]:
    """
    Optional stretch goal: align **3-month** and **10-year** Treasury *levels*
    on a common date grid and report **spread** (10Y − 3M) or inversion flags.

    This may require **new** FRED series IDs in ``config.py`` /
    ``FredProducer`` first; only implement the math here once those series exist
    as parallel sequences.

    Parameters
    ----------
    dates_and_3m, dates_and_10y
        Sorted (date, yield_pct) sequences (same units, e.g. percent points).

    Returns
    -------
    list[MetricRow]
        Empty until implemented.
    """
    # TODO(assigned teammate): align dates, compute spread / inversion days
    _ = dates_and_3m, dates_and_10y
    return []
