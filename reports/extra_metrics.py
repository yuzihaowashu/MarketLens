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
     if not ticker_to_daily_return_pct:
        return []

    returns = list(ticker_to_daily_return_pct.values())
    n = len(returns)

    up_count = sum(1 for r in returns if r > 0)
    breadth_up = up_count / n

    sorted_returns = sorted(returns)
    mid = n // 2
    if n % 2 == 1:
        median_return = sorted_returns[mid]
    else:
        median_return = (sorted_returns[mid - 1] + sorted_returns[mid]) / 2

    dispersion = max(returns) - min(returns)

    return [
        MetricRow(
            metric_id="breadth_up",
            label="Watchlist Up Ratio",
            value=round(breadth_up, 4),
            interpretation=f"{up_count} of {n} tracked tickers moved higher today.",
        ),
        MetricRow(
            metric_id="median_return",
            label="Median Daily Return (%)",
            value=round(median_return, 2),
            interpretation="This is the middle daily return across the watchlist, which helps summarize the typical move.",
        ),
        MetricRow(
            metric_id="dispersion",
            label="Return Dispersion (%)",
            value=round(dispersion, 2),
            interpretation="A wider max-minus-min return spread suggests more divergence and cross-sectional stress across tracked names.",
        ),
    ]

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
    if not latest_by_variable:
        return []

    rows: list[MetricRow] = []

    treasury_10y = latest_by_variable.get("TREASURY_10Y")
    inflation_expectations_10y = latest_by_variable.get("INFLATION_EXPECTATIONS_10Y")
    consumer_sentiment = latest_by_variable.get("CONSUMER_SENTIMENT")

    if treasury_10y is not None and inflation_expectations_10y is not None:
        real_yield_proxy = treasury_10y - inflation_expectations_10y
        if real_yield_proxy > 1:
            interpretation = (
                "The 10Y Treasury yield is materially above 10Y inflation expectations, "
                "suggesting relatively tighter real financial conditions."
            )
        elif real_yield_proxy < 0:
            interpretation = (
                "The 10Y Treasury yield is below 10Y inflation expectations, "
                "suggesting a negative real-yield proxy and relatively looser real conditions."
            )
        else:
            interpretation = (
                "The 10Y Treasury yield is close to 10Y inflation expectations, "
                "suggesting a roughly neutral real-yield proxy."
            )

        rows.append(
            MetricRow(
                metric_id="real_yield_proxy",
                label="10Y Real Yield Proxy",
                value=round(real_yield_proxy, 2),
                interpretation=interpretation,
            )
        )

    if consumer_sentiment is not None:
        if consumer_sentiment >= 90:
            sentiment_interpretation = (
                "Consumer sentiment is relatively firm by this simple threshold check."
            )
        elif consumer_sentiment >= 70:
            sentiment_interpretation = (
                "Consumer sentiment is in a middle range, suggesting a mixed household mood."
            )
        else:
            sentiment_interpretation = (
                "Consumer sentiment is relatively weak by this simple threshold check."
            )

        rows.append(
            MetricRow(
                metric_id="consumer_sentiment_level",
                label="Consumer Sentiment",
                value=round(consumer_sentiment, 2),
                interpretation=sentiment_interpretation,
            )
        )

    return rows

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
    if not ticker_to_volume or not ticker_to_avg_volume_20d:
        return []

    ratios = []
    for ticker, volume in ticker_to_volume.items():
        avg_volume = ticker_to_avg_volume_20d.get(ticker)
        if avg_volume is None or avg_volume <= 0:
            continue
        ratio = volume / avg_volume
        ratio = max(0.5, min(ratio, 5.0))
        ratios.append(ratio)

    if not ratios:
        return []

    avg_ratio = sum(ratios) / len(ratios)
    max_ratio = max(ratios)
    min_ratio = min(ratios)

    if avg_ratio > 1.2:
        avg_interpretation = (
            "Average watchlist volume is running above the 20-day baseline, "
            "suggesting elevated trading activity today."
        )
    elif avg_ratio < 0.8:
        avg_interpretation = (
            "Average watchlist volume is running below the 20-day baseline, "
            "suggesting lighter-than-usual trading activity today."
        )
    else:
        avg_interpretation = (
            "Average watchlist volume is close to the 20-day baseline, "
            "suggesting fairly normal trading activity today."
        )

    return [
        MetricRow(
            metric_id="watchlist_avg_volume_ratio",
            label="Watchlist Avg Volume Ratio",
            value=round(avg_ratio, 2),
            interpretation=avg_interpretation,
        ),
        MetricRow(
            metric_id="watchlist_max_volume_ratio",
            label="Highest Volume Ratio",
            value=round(max_ratio, 2),
            interpretation="This is the strongest single-name volume surge versus its own 20-day average.",
        ),
        MetricRow(
            metric_id="watchlist_min_volume_ratio",
            label="Lowest Volume Ratio",
            value=round(min_ratio, 2),
            interpretation="This is the weakest single-name trading activity versus its own 20-day average.",
        ),
    ]


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
    if not dates_and_3m or not dates_and_10y:
        return []

    curve_3m = {dt: value for dt, value in dates_and_3m}
    curve_10y = {dt: value for dt, value in dates_and_10y}

    common_dates = sorted(set(curve_3m) & set(curve_10y))
    if not common_dates:
        return []

    latest_date = common_dates[-1]
    latest_3m = curve_3m[latest_date]
    latest_10y = curve_10y[latest_date]
    latest_spread = latest_10y - latest_3m

    inversion_days = 0
    for dt in common_dates:
        if curve_10y[dt] - curve_3m[dt] < 0:
            inversion_days += 1

    if latest_spread < 0:
        spread_interpretation = (
            f"On {latest_date.isoformat()}, the 10Y minus 3M Treasury spread was negative, "
            "which indicates an inverted yield curve on the latest aligned date."
        )
    elif latest_spread == 0:
        spread_interpretation = (
            f"On {latest_date.isoformat()}, the 10Y minus 3M Treasury spread was flat, "
            "suggesting the curve was approximately neutral on the latest aligned date."
        )
    else:
        spread_interpretation = (
            f"On {latest_date.isoformat()}, the 10Y minus 3M Treasury spread was positive, "
            "so the curve was not inverted on the latest aligned date."
        )

    if inversion_days == 0:
        inversion_interpretation = (
            "No inversions were observed across the aligned sample."
        )
    else:
        inversion_interpretation = (
            f"The aligned sample contains {inversion_days} inversion day(s), "
            "counting dates where the 10Y yield was below the 3M yield."
        )

    return [
        MetricRow(
            metric_id="term_spread_10y_3m",
            label="10Y Minus 3M Spread",
            value=round(latest_spread, 2),
            interpretation=spread_interpretation,
        ),
        MetricRow(
            metric_id="term_curve_inversion_days",
            label="Inversion Days",
            value=inversion_days,
            interpretation=inversion_interpretation,
        ),
    ]
