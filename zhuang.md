# Zhuang — Extension metrics

Hi Zhuang,

## What this work is about (big picture)

**MarketLens** is our class project to help someone understand what’s going on in markets and the wider economy — not with a wall of jargon, but with clear summaries and charts.

The project includes behind-the-scenes pieces: prices and indicators are refreshed on a schedule, stored, and shown on the site. **This assignment is one entry point** — a smaller area where already-prepared numbers become a few plain-language lines, the kind a friend might add in the margin: for example, whether most of the stocks we follow moved the same way on a given day, whether trading looked busier or calmer than usual, or how two straightforward economy readings look side by side (described fairly, with humility about what they can’t prove).

**You’re welcome to implement whenever it feels right for you** — a little at a time is great, and so is stretching further once you’re comfortable. You don’t need to rebuild the website or the nightly run; those are already wired. Your additions are **optional readouts**: short takeaways that sit on top of what we hand you. The notes below point to one file as an easy first step; if you later want to explore other parts of the repo or suggest new ideas, we’d love that too.

---

## Scope

| File | Suggestion |
|------|------------|
| `reports/extra_metrics.py` | Implement the `TODO` sections when you feel ready |

To keep things easy for you (and light on merge conflicts), **staying inside `reports/extra_metrics.py` for now** is usually enough. The Streamlit side is already connected: whenever a hooked function returns a non-empty `list[MetricRow]`, **Stock Deep Dive** picks it up and shows an **“Extension metrics”** block. If you’re still experimenting or returning empty lists, that block just stays out of the way — that’s completely fine and expected while you’re learning.

---

## What gets passed in (you don’t have to write SQL here)

The app gathers snapshots and calls:

1. **`watchlist_breadth_from_daily_returns(ticker_to_daily_return_pct)`**  
   - Keys: uppercase tickers (e.g. `AAPL`).  
   - Values: **percent units** (`0.42` ⇒ +0.42% for the day, not `0.0042`).  
   - From the latest day in `V_ANOMALY_SCORES` (scaled in the app).

2. **`fred_macro_spread_metrics(latest_by_variable)`**  
   - Keys: **VARIABLE** names from `RAW_FRED_INDICATORS`.  
   - Values: latest float for each variable.

3. **`liquidity_proxy_from_volumes(ticker_to_volume, ticker_to_avg_volume_20d)`**  
   - Uppercase ticker keys; latest volume and ~20-day average volume from `V_STOCK_PRICES`.

4. **`term_structure_kink_signal(...)`** (optional stretch)  
   - Not wired to the UI yet — okay to sketch or skip until you’re curious.  
   - Args: ordered `(date, float)` series.

---

## Output shape (when you choose to fill things in)

Each row is a **`MetricRow`** in the same file: `metric_id`, `label`, `value`, `interpretation`.  
Return **`list[MetricRow]`**, or **`[]`** whenever you prefer not to show anything yet.

---

## Ideas — only if they sound fun

Breadth across the watchlist, simple macro combinations (with honest caveats in `interpretation`), volume vs average — all fair game. These are for **learning and narrative** in class, not trading advice; gentle, descriptive wording is perfect.

---

## A small test command

From `MarketLens/`:

```bash
python -m pytest tests/test_extra_metrics.py -q
```

It only checks types; Snowflake isn’t required.

---

## See also (whole pipeline, read-only)

- **`docs/data_journey.md`** — short narrative + Mermaid diagram from sources → Snowflake → dbt / signals → Airflow / Streamlit.  
- **`python scripts/pipeline_overview.py`** — prints how this machine resolves `config.py` (no database calls). Add `--json` for machine-readable output.  
- **`queries/exploratory/`** — SQL snippets for Snowsight only; not executed by the DAG.

---

## Whenever you’d like to share work

A branch and a small PR are lovely when you’re comfortable — no rush. If a question comes up, drop it in chat and @ whoever usually touches the app; linking this file is enough context.

Take your time getting oriented. We’re happy you’re here.
