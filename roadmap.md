# MarketLens — Where We Are & Where We Could Go

---

## Project at a Glance

MarketLens started as a question: *can we make financial market data understandable for everyday people, not just Wall Street analysts?*

The answer we're building toward is a fully automated data pipeline that watches the market every day, detects unusual movements, and explains them in plain English — at whatever level of detail the user wants. A beginner gets a simple "AAPL moved a lot today because...". An analyst gets Z-scores, yield curve inversions, and macro context.

What makes this interesting from a data engineering perspective is that it's not just a dashboard. It's a system with moving parts: a scheduler that wakes up every evening, producers that fetch data from multiple sources, a warehouse that stores and transforms that data, signals that detect patterns, and a notification layer that pushes results to the right people. Each of those pieces is a real tool used in production data teams.

---

## How Far Along Are We?

The honest answer: **the core of what we set out to build is done.** Everything below the line marked "core" is additive — each direction makes the system richer, more interesting, or better suited to class requirements, but none of them are needed for the pipeline to run correctly today.

```
 Core pipeline (Phases 0 → 1 → 4)          ████████████████████  100%
 Kafka streaming (Phase 2)                  ██████████████░░░░░░   70%  (stack + consumers; daily ingest still mostly direct → Snowflake)
 New data sources (FRED, EDGAR, etc.)         █████████████░░░░░░░   65%  (FRED + SEC/Cortex path in DAG; widen series, fallbacks, news)
 New signals (RSI, MA crossover, etc.)      ███████████████░░░░░   75%  (SQL + dbt models for momentum, trend, drawdown, sector, macro)
 dbt (staging / marts / tests in repo)      ███████████████░░░░░   75%  (coexists with legacy `signals/` — converge + tests in CI next)
 Spark / Flink (optional class depth)       ░░░░░░░░░░░░░░░░░░░░    0%  (not started; Snowflake SQL covers current scale)
 Airflow depth (weekly DAG, data quality)   ██████████░░░░░░░░░░   50%  (daily DAG + FRED/SEC/logging; weekly + DQ gates still open)
 Dashboard UX (export, comparison, etc.)    ███████████░░░░░░░░░   55%  (rich multi-page app + streaming views; export/watchlist TBD)
 Notifications (dedup, digest, SMS)         █████████████░░░░░░░   65%  (Slack/email + streaming-side notifier; dedup/digest TBD)
 Testing & DevOps (CI, Docker, coverage)    █████████░░░░░░░░░░░   45%  (94 pytest tests locally; GitHub Actions workflow not in repo yet)
```

*The percentages above are directional estimates aligned with the repo as of **April 2026** — not automated metrics.*

### What "core complete" means in practice

Three people, three months, one working system:
- A **live daily pipeline** that ingests watchlist prices, Snowflake Marketplace macro series, **FRED API** series (when `FRED_API_KEY` is set), and **SEC EDGAR** metadata/text with optional Cortex summaries
- A **Snowflake warehouse** with layered SQL signals — classic return/volatility/Z-score views plus **RSI, moving-average crossovers, drawdown, sector rotation, yield-curve and macro narratives**, implemented both under `signals/` and as **dbt** staging/mart models
- An **Airflow DAG** that orchestrates ingestion → signals → anomaly detection → notifications, with **run logging** to `PIPELINE_RUN_LOG`
- A **Streamlit dashboard** with **multiple** pages (chat via Cortex, deep dives, macro/streaming-oriented views, pipeline health) that keeps evolving with the pipeline
- **Slack and email alerts** when anomalies are detected, with fail-open design, plus optional **Kafka-anchored** consumers for dashboards and follow-up notifications
- **94 `pytest` cases**, runnable without a live Snowflake session for most paths
- Engineering patterns used in real production teams: circuit breakers, fallback chains, idempotent writes, pipeline observability

### What's left and what it takes

The remaining directions fall into three natural groups based on how much new ground they cover.

**Quick wins — no new tools needed**
These build entirely on what's already in place. Anyone comfortable with the existing codebase can pick one up independently.
- *Extend data sources* — **FRED and SEC producers are already wired into the daily DAG**; next steps are adding more FRED series, optional fallbacks (e.g. AlphaVantage), and richer news/sentiment feeds — still subclasses of `BaseProducer` with the same retry/fallback patterns.
- *Enrich the signal library* — **RSI, MA crossover, sector rotation, and drawdown** already exist in SQL/dbt; keep going with correlation matrices, earnings-calendar overlays, or cross-asset signals on top of tables we already land.
- *Polish the dashboard* — add ticker comparison, CSV export, a watchlist editor, and an alert history page. Pure Streamlit additions, no backend changes.
- *Harden the pipeline* — add a data quality check task in Airflow (assert row counts and no null prices before signals refresh), set up GitHub Actions so tests run automatically on every pull request.

**Medium lift — one new tool to learn**
Each of these introduces one new concept or tool that requires some ramp-up but pays off quickly.
- *Finish Kafka as the default ingest path* (Phase 2) — **Kafka, `kafka-python`, Docker Compose, and several consumers already exist.** The main remaining work is making the **daily DAG** publish prices (and optionally macro) to Kafka first, running durable **Snowflake writers** off the topics, and proving lag/DLQ behavior under failure — i.e. closing the loop from "optional streaming stack" to "default production path."
- *Deepen dbt adoption* — a **dbt project already mirrors much of `signals/`** with staging/marts and tests. Next is picking a single source of truth (dbt vs raw SQL files), wiring `dbt test`/`dbt docs` into CI, and deleting duplicate definitions once parity is proven.
- *Deepen Airflow usage* — add a weekly summary DAG, move Snowflake credentials into Airflow Connections (the production-standard way), and enable backfill so we can populate historical data. Each sub-task is independent.

**Significant investment — steep learning curve**
These are genuinely new engineering territory. They're the right choice if the goal is hands-on experience with Spark or Flink for class, not because the pipeline needs them for performance at our current data scale.
- *Apache Spark for batch signal computation* — re-implement rolling volatility and Z-scores as a PySpark job that reads from `RAW_STOCK_PRICES` and writes enriched results back to Snowflake. Teaches Spark DataFrames, window functions, and reading/writing Snowflake from Spark. Note: this replaces work Snowflake SQL already does well — the value is the learning experience.
- *Apache Flink for real-time streaming* — build a Flink job that consumes from the Kafka `raw.stock.prices` topic and computes a rolling Z-score in near-real-time, emitting intraday anomaly alerts rather than waiting until 6 PM. Yahoo Finance-style sources are delayed and not a true tick stream, so Flink exercises need a **lawful, ToS-compliant** feed (e.g. a **licensed market-data vendor** you are allowed to use) or, for pure classroom practice, **replay/synthetic ticks** you generate from historical files into Kafka so no third-party gray-area scraping is involved. Introduces event-time processing, watermarks, and stateful streaming operators.

The takeaway: most of what's left is **low-effort and free**, and the two heavyweight items (Spark, Flink) are worth doing specifically to practice those tools in a real context — not because the pipeline is slow or broken without them.

---

### How to split this across three people

A clean way to divide the work is by **layer** — each person owns one layer of the system end-to-end. This minimizes blocking (you're rarely waiting on each other), gives each person a coherent story to tell about what they built, and ensures all three class tools (Spark, Flink, dbt) are covered across the group.

---

#### Person A — Data Layer: *Richer Inputs, Better Transforms*
*Own what goes into the system and how it gets shaped.*

The data layer is about expanding where our data comes from and how cleanly it flows into Snowflake. **Yahoo Finance, Snowflake Marketplace, FRED, and SEC EDGAR are already integrated** (with more series and quality hardening still to do). There's still a much richer world of free data — the Federal Reserve's FRED catalog alone has 800,000+ series. Person A would extend those sources, keep enriching the signal library, and drive **dbt** toward being the single documented, tested source of truth alongside (and eventually instead of) hand-maintained `signals/*.sql`. This is the most SQL-and-Python-heavy role, with only light infrastructure touchpoints.

| Task | What it involves |
|---|---|
| FRED API producer | **Shipped:** `FredProducer` + `ingest_fred` in the daily DAG — extend to more series and harden empty-key behavior |
| SEC EDGAR producer | **Shipped:** metadata → text → Cortex summaries in DAG; deepen coverage, rate limits, and narrative quality |
| New SQL signals | **Many shipped** in `signals/` and dbt — add correlation / earnings-calendar style signals next |
| dbt migration | **In progress:** dbt models + tests exist — finish parity, CI, and retire duplicate `signals/*.sql` where dbt is authoritative |

**What you'd learn:** FRED/EDGAR API integration, SQL window functions for financial signals, dbt (industry-standard SQL transformation tool).

---

#### Person B — Pipeline Layer: *Reliable, Scalable Orchestration*
*Own how data moves from ingestion through to Snowflake.*

The pipeline layer is about making data movement more robust and scalable. The biggest item here is **finishing Kafka as the default ingest path** — brokers, topics, Python producers/consumers, and helper scripts exist, but the **daily DAG still writes most raw data straight to Snowflake.** From there, Person B would deepen the Airflow setup (a weekly summary DAG, data quality checks before signals refresh, backfill for historical data) and take on **Apache Spark** as the heavyweight class exercise — re-implementing signal computation as a distributed batch job.

| Task | What it involves |
|---|---|
| Activate Kafka (Phase 2) | **Infra + consumers exist** — switch the DAG's ingest path to publish → consume → Snowflake and verify lag, retries, DLQ in Kafka UI |
| Airflow weekly DAG | New `marketlens_weekly.py` DAG that runs Monday mornings, computes 30-day rolling stats, emails a market recap |
| Airflow data quality task | Add a task between `ingest_prices` and `refresh_signals` that asserts row counts > 0 and no null prices — fail fast rather than propagating bad data |
| Airflow backfill | Set `catchup=True` and a `start_date` to populate `RAW_STOCK_PRICES` with historical data for backtesting |
| Apache Spark batch job | Re-implement rolling volatility and Z-score computation as a PySpark job that reads `RAW_STOCK_PRICES` and writes back to Snowflake. The value is learning Spark DataFrames and window functions, not performance |

**What you'd learn:** Kafka producer/consumer architecture, Airflow SLAs and backfill, Apache Spark DataFrames and the Snowflake-Spark connector.

---

#### Person C — Application Layer: *What Users See and System Reliability*
*Own the dashboard, alerts, and everything that makes the system dependable.*

The application layer is about the experience on both ends — what users see in the dashboard and what the team sees when something goes wrong. Person C would extend the Streamlit dashboard with features users have been asking for (ticker comparison, CSV export, watchlist editing), improve the notification system (deduplication, digests), set up CI so tests run automatically on every pull request, and take on **Apache Flink** as the real-time streaming exercise. Flink naturally pairs with Person B's Kafka work — once Kafka is live, Person C can build the Flink job on top of it.

| Task | What it involves |
|---|---|
| Dashboard: ticker comparison | Overlay two tickers on the same Z-score chart |
| Dashboard: CSV export | Download button on the Deep Dive page |
| Dashboard: watchlist editor | Add/remove tickers from the sidebar, persist in Snowflake `USER_PREFERENCES` |
| Dashboard: alert history | Query a new `ALERT_LOG` table to show every anomaly alert ever sent |
| Notification deduplication | Before sending, check if the same ticker + signal was alerted in the last 24 hours; skip duplicates |
| Notification digest mode | Batch all daily anomalies into one summary message instead of one alert per signal |
| GitHub Actions CI | One workflow file: `pytest tests/` runs automatically on every pull request |
| Apache Flink streaming job | Consume from Kafka `raw.stock.prices`, compute a rolling Z-score in near-real time, emit intraday anomaly events. Pair with a **compliant** streaming source or **Kafka replay** of historical ticks for learning |

**What you'd learn:** Streamlit advanced patterns, alert system design, GitHub Actions CI/CD, Apache Flink event-time processing and watermarks.

---

#### At a glance

```
                   Person A              Person B              Person C
                 Data Layer           Pipeline Layer        Application Layer
                ────────────          ──────────────        ─────────────────
  Data in:     FRED + EDGAR           Kafka (Phase 2)       —
  Transforms:  New SQL signals        Spark batch job       —
               dbt models
  Orchestration: —                    Airflow depth         —
  Streaming:   —                      —                     Flink (real-time)
  User-facing: —                      —                     Dashboard UX
  Reliability: —                      —                     CI + Notifications
  ────────────  ────────────          ──────────────        ─────────────────
  Class tools:      dbt                   Spark                  Flink
```

One dependency to be aware of: Person C's Flink job reads from the Kafka topic that Person B activates. Person C can start on dashboard work while Person B sets up Kafka, then pick up Flink once the topic is live. Everything else across the three tracks is fully independent.

---

## What We've Built So Far

### Tools from class — already implemented

| Tool | How we use it |
|---|---|
| **Apache Airflow** | Schedules and orchestrates the full pipeline on a daily cadence. The DAG runs every weekday at 6 PM ET, handles task dependencies, and retries on failure |
| **Snowflake** | Central data warehouse. Stores raw ingested data, hosts signal computation as SQL views and **dbt-backed marts**, and powers the dashboard queries |
| **Kafka** | **Phase 2 stack is real:** `docker-compose.kafka.yml`, `kafka-python`, topic producers, and multiple Python consumers (e.g. tick feed, dashboard, anomaly, notifier paths). The **daily batch ingest** still primarily uses direct Snowflake writes — wiring the DAG through Kafka by default is the remaining orchestration step |

### What the pipeline does today

Every weekday evening, the Airflow DAG (high level):
1. Fetches stock prices for the configured watchlist from Yahoo Finance
2. Fetches macro indicators (Fed Funds Rate, CPI, etc.) from Snowflake Marketplace
3. When `FRED_API_KEY` is set, lands additional macro series via the **FRED API** into `RAW_FRED_INDICATORS`
4. When `SEC_USER_AGENT` is set, walks **SEC EDGAR** metadata → filing text → optional **Cortex** summaries
5. Refreshes signal logic in Snowflake — returns, volatility, Z-scores, macro/yield-curve context, **RSI / MA / drawdown / sector rotation**, and related dbt models where deployed
6. Checks for anomalies (e.g. elevated Z-scores) and sends **Slack and/or email** alerts if configured
7. Logs each task to `PIPELINE_RUN_LOG` for observability

The Streamlit dashboard is **multi-page** — always evolving, but centered on Cortex chat, equity deep dives, macro overlays, pipeline health, and **streaming-oriented** views when Kafka-backed feeds are running.

### Engineering patterns we applied

- **Circuit breaker** on each data producer — if a source fails repeatedly, it pauses automatically rather than slowing the whole pipeline
- **Fallback chain** — producers are tried in priority order; if Yahoo Finance is down, the system can fall back to another source
- **Idempotent writes** via `MERGE INTO` — safe to re-run the pipeline without creating duplicate rows
- **Fail-open notifications** — if Slack is down, email still goes out; one broken channel never blocks the others
- **94 `pytest` tests** covering producers, notifications, DAG wiring, dbt project structure, and more — runnable without a live Snowflake connection for most suites

---

## What We Could Explore Next

These are directions, not assignments. Each one could be a standalone feature, a learning exercise, or a jumping-off point for a bigger idea. We can pick whatever sounds most interesting or most relevant to what we're covering in class.

### Cost legend
> `FREE` — no account or API key needed
> `FREE TIER` — free up to a daily/monthly limit, enough for this project
> `PAID` — requires a subscription or per-use billing

---

### Direction 1 — Complete the Kafka Streaming Layer
*The code is already there. This is about turning Phase 1 into Phase 2.*

Right now **batch ingestion in the daily DAG still writes directly from producers to Snowflake** for most paths. The more scalable pattern — and the one Kafka is designed for — is to decouple the producer from the writer: the producer publishes a message; a separate consumer handles the Snowflake write. That makes the pipeline more resilient and opens the door to multiple consumers reading the same stream (several experimental consumers already exist in `ingestion/`).

- [x] `FREE` **`kafka-python` + local Kafka stack** — dependency is enabled in `requirements.txt`; bring brokers up with `docker compose -f docker-compose.kafka.yml up -d` (see comments in the compose file for Kafka UI, etc.)
- [ ] `FREE` Switch `_ingest_prices()` in the DAG to use `fetch_and_publish_to_kafka` instead of `fetch_and_write_to_snowflake`, with a matching consume-to-Snowflake task in the graph
- [ ] `FREE` Run the **Snowflake prices consumer** as a long-lived service (today you can start it from the host via `start_streaming.sh`; optionally add it as a `docker-compose` service for demos)
- [ ] `FREE` Add a dead-letter topic (`raw.stock.prices.dlq`) for messages that fail to write after N retries
- [ ] `FREE` Surface Kafka consumer lag on the Pipeline Health dashboard page

---

### Direction 2 — Richer Data Sources
*More data means richer signals. All the best options here are free.*

**FRED** (800,000+ public series) and **SEC EDGAR** are already first-class inputs when API keys / user-agents are configured — see `ingestion/fred_producer.py`, `ingestion/sec_producer.py`, and the `ingest_fred` / `ingest_sec_*` / `summarize_sec_filings` tasks in `dags/marketlens_daily.py`. The opportunity now is to **widen coverage** (more FRED IDs, richer filing sets), add **fallback vendors**, and layer **news/sentiment** on top.

- [x] `FREE` **FRED API producer** — **Done (v1):** `FredProducer` lands core macro series into `RAW_FRED_INDICATORS` and is orchestrated by `ingest_fred`. Keep iterating on series lists, documentation, and tests
- [x] `FREE` **SEC EDGAR producer** — **Done (v1):** metadata + primary-document text + Cortex summaries into Snowflake, gated on `SEC_USER_AGENT`. Keep hardening politeness/rate limits and narrative quality
- [ ] `FREE TIER` **AlphaVantage fallback** — 25 free calls/day; useful as a priority-2 fallback behind Yahoo Finance, especially for intraday data. Free key at alphavantage.co
- [ ] `FREE TIER` **News sentiment** — fetch headlines from NewsAPI (100 free/day) or Finnhub (60 calls/min free) and score sentiment using Snowflake Cortex. Store in a `RAW_NEWS_SENTIMENT` table and add a sentiment signal view
- [ ] `PAID` **Polygon.io** — real-time quotes and options flow data; no meaningful free tier for historical OHLCV, but worth knowing about for when the project scales

---

### Direction 3 — New Signals & Analytics
*Pure SQL views in Snowflake — no new infrastructure, no extra cost.*

Many of these already exist as SQL under `signals/` and/or dbt models under `dbt/models/**`. The list below doubles as a **backlog** — checked items are implemented in-repo; unchecked items are still great next steps.

- [x] `FREE` **RSI (Relative Strength Index)** — a classic momentum indicator; flag tickers above 70 (overbought) or below 30 (oversold) using SQL window functions
- [x] `FREE` **Moving average crossover** — detect golden cross (20-day MA crosses above 50-day MA) and death cross events; historically meaningful trend signals
- [x] `FREE` **Sector rotation** — compare sector ETF returns (XLK tech, XLF finance, XLE energy, etc.) to see which sectors are leading vs. lagging the S&P 500
- [ ] `FREE` **Correlation matrix** — compute pairwise return correlations across the watchlist to surface which stocks move together and which are truly independent
- [x] `FREE` **Drawdown tracker** — how far each ticker is from its 52-week high; a simple but useful risk metric
- [ ] `FREE` **Earnings calendar** — mark earnings report dates so the anomaly detector can distinguish "stock moved because earnings" from a genuinely unexplained anomaly

---

### Direction 4 — Spark, Flink, and dbt
*The tools we're learning in class, applied to a real problem — but let's be honest about when each one makes sense.*

This is probably the most directly relevant direction for CSE-5114, so it's worth thinking carefully about *why* we'd use each tool rather than just adding them for the sake of it.

**An important insight about Spark and app performance:**
We noticed the dashboard can feel slow to load, and it might be tempting to think "let's add Spark to speed it up." But Spark would actually do nothing here — the slowness isn't a computation problem. Our bottleneck is Snowflake's warehouse cold start (it suspends after a few minutes of inactivity, and the first query waits 15–30 seconds for it to wake up), plus small inefficiencies in how the app manages its database connection. We've already fixed those in the code — caching the connection with `st.cache_resource`, removing an unnecessary `SELECT 1` round-trip before every query, and moving cached query functions to module level so they persist across page switches. The app should feel noticeably snappier now.

So when would Spark and Flink *actually* make sense for us?

- **Spark** becomes worth it when our data volume grows large enough that Snowflake SQL starts taking too long — think 500+ tickers, tick-by-tick intraday data (millions of rows per day), or training ML models on years of price history. At our current scale of 9 tickers with daily OHLCV data, Snowflake handles the computation in milliseconds. The right reason to try Spark now is to *learn it*, not because we need it.

- **Flink** becomes worth it when we want *intraday* alerts instead of end-of-day ones — detecting "NVDA is up 4% in the last 20 minutes" in near-real-time rather than at 6 PM. But Flink needs a real-time data feed to consume from. Yahoo Finance (our current source) has a 15-minute delay and doesn't stream. For production-style work, use only **data you have the right to use** (vendor contract, exchange policy, course-provided sandbox, etc.). For a **safe learning path**, replay your own historical bars or synthetic ticks into Kafka and let Flink consume that — same APIs and concepts, no reliance on legally or contractually questionable feeds.

- **dbt** is the most immediately useful of the three. A **dbt Core project already lives under `dbt/`** with staging and mart models plus tests — the remaining work is **consolidation** (decide whether dbt or `signals/*.sql` is canonical), richer documentation, and **CI** so `dbt test` runs on every PR.

- [x] `FREE` **dbt models** — **In progress:** staging/marts/tests exist and mirror much of the legacy SQL. Next steps: finish parity, run `dbt docs`/`dbt test` in automation, and delete duplicate definitions where dbt is authoritative
- [ ] `FREE` **Apache Spark batch job** — re-implement rolling volatility and Z-score computation as a PySpark job that reads from `RAW_STOCK_PRICES` and writes to a `SIGNALS_STOCK` table. Good hands-on practice with Spark DataFrames and window functions. The right reason to do this is learning, not performance — our current Snowflake views are already fast enough
- [ ] `FREE` **Apache Flink streaming job** — consume from the Kafka `raw.stock.prices` topic in real time and compute a rolling Z-score as messages arrive. Use a **compliant** streaming vendor if you have one, or drive the topic with a **controlled replay/synthetic producer** for class demos. Demonstrates Flink's event-time processing and watermarks, and produces near-real-time anomaly alerts rather than end-of-day ones

---

### Direction 5 — Airflow Depth
*Making the orchestration layer more production-grade.*

- [ ] `FREE` **Weekly summary DAG** — a second DAG (`marketlens_weekly`) that runs Monday mornings, computes 30-day rolling stats, and emails a market recap. Good practice writing a second DAG from scratch
- [ ] `FREE` **Data quality task** — add a task between `ingest_prices` and `refresh_signals` that asserts the raw data looks sane (row count > 0, no null close prices, no obviously wrong values). Fail fast rather than propagating bad data downstream
- [ ] `FREE` **Airflow Connections** — move Snowflake credentials from `.env` into an Airflow Connection object (Admin → Connections in the UI) and use `SnowflakeHook`. This is how production Airflow deployments manage credentials
- [ ] `FREE` **Backfill support** — turn on `catchup=True` and set a `start_date` to populate `RAW_STOCK_PRICES` with historical data. Useful for backtesting signals

---

### Direction 6 — Dashboard & User Experience
*Making the app more useful and polished.*

A note on performance: we've already applied several fixes that should make the app feel faster — the Snowflake connection is now cached and shared across all sessions using `st.cache_resource`, the unnecessary health-check round-trip before every query has been removed, and all the data-fetching functions have been moved to module level so their caches persist properly across page switches. The one thing still worth doing manually is increasing the Snowflake warehouse auto-suspend timeout from 1 minute to 10–15 minutes in the Snowflake web UI (Admin → Warehouses) — this prevents the cold-start delay on first page load during active work sessions.

- [ ] `FREE` **Watchlist editor** — let users add or remove tickers from the sidebar; persist preferences in a Snowflake `USER_PREFERENCES` table
- [ ] `FREE` **Ticker comparison mode** — overlay two tickers on the same Z-score chart to compare their anomaly patterns side by side
- [ ] `FREE` **Export to CSV** — a download button on the Deep Dive page so users can take the data elsewhere
- [ ] `FREE` **Alert history page** — a log of every anomaly alert ever sent, queryable by ticker, date, and signal type
- [ ] `FREE` **Dark mode** — a light/dark toggle via `.streamlit/config.toml`

---

### Direction 7 — Notifications
*Reaching people where they are.*

- [ ] `FREE` **Alert deduplication** — check `ALERT_LOG` before sending; skip if the same ticker + signal was already alerted in the past 24 hours. Prevents notification fatigue
- [ ] `FREE` **Digest mode** — batch all daily anomalies into one summary message instead of one alert per signal
- [ ] `FREE TIER` **PagerDuty** — free developer tier available; useful for pipeline failure alerts (not just anomaly alerts)
- [ ] `PAID` **Twilio SMS** — charged per message; easy to add using the existing `BaseSender` pattern

---

### Direction 8 — Testing & DevOps
*Making the codebase more reliable and easier to contribute to.*

- [ ] `FREE` **`test_macro_producer.py`** — the macro producer is the only major component without unit tests; worth adding before the source gets more complex
- [ ] `FREE` **GitHub Actions CI** — one workflow file so `pytest` runs automatically on every pull request; free for public repos
- [ ] `FREE` **Full Docker Compose** — add Airflow and the Streamlit app to `docker-compose.kafka.yml` so the entire stack starts with one command
- [ ] `FREE` **Structured logging** — replace `print()` calls with Python `logging` in JSON format, piped into `PIPELINE_RUN_LOG` or a local file
- [ ] `FREE` **Terraform for Snowflake** — define the schema, tables, and role grants as code so any teammate can recreate the Snowflake environment from scratch
- [ ] `PAID` **Secrets manager** — AWS Secrets Manager (paid) or HashiCorp Vault (free, self-hosted) to replace the `.env` file in a production deployment

---

## Summary Table

| Direction | What you'd learn | All free? |
|---|---|---|
| Kafka Phase 2 | Stream processing, decoupled architecture | Yes |
| New data sources | API integration, producer pattern | Mostly (FRED + EDGAR fully free) |
| New signals | SQL window functions, financial indicators | Yes |
| Spark / Flink / dbt | Core class tools applied to a real pipeline | Yes |
| Airflow depth | DAG design, data quality, credential management | Yes |
| Dashboard & UX | Streamlit, user experience design | Yes |
| Notifications | Alert design, deduplication patterns | Mostly |
| Testing & DevOps | CI/CD, infrastructure as code | Mostly |
