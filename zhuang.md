# Zhuang — Extension metrics

Hi Zhuang,

## What this work is about

**MarketLens** is our class project. It helps people understand markets and the wider economy through clear summaries and charts instead of dense jargon.

The app ingests stock prices and economic indicators on a schedule, stores them, and displays the results. **This track is a good entry point for you**: a single, well-scoped area where the numbers are already prepared and your role is to turn them into short, plain-English takeaways. Picture short margin notes from a friend—e.g., whether most tracked stocks moved together that day, whether trading looked busier or quieter than usual, or how two simple macro readings compare when you describe them honestly (including what they **do not** prove).

**You can expand in depth or breadth**—more metrics in the same file, richer `interpretation` text, exploratory SQL, or tests. For anything larger, use a branch and a pull request, and sync with the team before changing the main Streamlit app or the Airflow DAG. The first step stays small on purpose so the repository stays easy to navigate. You are layering optional readouts on top of data we supply; the website and the nightly pipeline are already implemented and wired up.

---

## Where to begin—and how to grow

| Phase | What you might do |
|-------|-------------------|
| **Start** | Complete the `TODO` sections in `reports/extra_metrics.py` so **Stock Deep Dive** shows the **“Extension metrics”** section when your functions return rows (the UI hook is already in place). |
| **Grow** | Add more `MetricRow` entries, refine labels and interpretations, or flesh out the optional `term_structure_kink_signal` helper (a separate UI hook can be added later if you want it visible). |
| **Stretch** | Add small companion modules under `reports/`, extend `queries/exploratory/`, or propose new readouts—we can help wire anything that needs a new entry point in the app. |

Working first in **`reports/extra_metrics.py`** limits merge friction. **Additional work in the same area is encouraged**, not only the minimum.

---

## Scope (primary file)

| File | Action |
|------|--------|
| `reports/extra_metrics.py` | Implement the `TODO` sections |

When a connected function returns a non-empty `list[MetricRow]`, **Stock Deep Dive** renders the **“Extension metrics”** block. Return **`[]`** to keep that block hidden.

---

## Inputs (no SQL required on your side)

The app builds snapshots and calls:

1. **`watchlist_breadth_from_daily_returns(ticker_to_daily_return_pct)`**  
   - Keys: uppercase tickers (e.g. `AAPL`).  
   - Values: **percent units** (`0.42` means +0.42% for the day, not `0.0042`).  
   - Source: latest trading day in `V_ANOMALY_SCORES` (values are scaled in the app).

2. **`fred_macro_spread_metrics(latest_by_variable)`**  
   - Keys: **VARIABLE** names from `RAW_FRED_INDICATORS`.  
   - Values: latest float for each variable.

3. **`liquidity_proxy_from_volumes(ticker_to_volume, ticker_to_avg_volume_20d)`**  
   - Keys: uppercase tickers.  
   - Values: latest-day volume and roughly 20-day average daily volume from `V_STOCK_PRICES`.

4. **`term_structure_kink_signal(...)`** (optional)  
   - Not connected to the UI yet—implement, stub, or skip.  
   - Arguments: ordered sequences of `(date, float)`.

---

## Output format

Each metric is a **`MetricRow`** in the same module: `metric_id`, `label`, `value`, `interpretation`.  
Functions return a **`list[MetricRow]`**; use an **empty list** to hide the extension block.

---

## Ideas (same tone, as many rows as help the story)

Watchlist breadth, simple macro combinations (with clear caveats in `interpretation`), volume versus its average—all valid. **Several `MetricRow` objects per function are fine** if they improve the narrative. These readouts support **learning and storytelling** in class, not trading advice; keep the wording descriptive and neutral.

---

## Tests

From the `MarketLens/` directory:

```bash
python -m pytest tests/test_extra_metrics.py -q
```

The suite checks return types only; Snowflake is not required.

---

## See also (pipeline context, read-only)

- **`docs/data_journey.md`** — Short walk-through plus a Mermaid diagram: sources → Snowflake → dbt / signals → Airflow / Streamlit.  
- **`python scripts/pipeline_overview.py`** — Prints resolved settings from `config.py` (no database connection). Use `--json` for machine-readable output.  
- **`queries/exploratory/`** — Example SQL for Snowsight; not executed by the DAG.

---

## Sharing work

Open a branch and submit a pull request so review stays simple. If anything is ambiguous, message the team and @ whoever owns the Streamlit side; linking to this file is enough background.

**This path is meant as a clear starting point—and we welcome you to build more on top of it.** Glad to have you on the team.
