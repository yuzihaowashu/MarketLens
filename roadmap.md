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
 Kafka streaming (Phase 2)                  ████████░░░░░░░░░░░░   40%  (code exists, not activated)
 New data sources (FRED, EDGAR, etc.)       ░░░░░░░░░░░░░░░░░░░░    0%
 New signals (RSI, MA crossover, etc.)      ░░░░░░░░░░░░░░░░░░░░    0%
 Spark / Flink / dbt                        ░░░░░░░░░░░░░░░░░░░░    0%
 Airflow depth (weekly DAG, data quality)   ████░░░░░░░░░░░░░░░░   20%
 Dashboard UX (export, comparison, etc.)    ██████░░░░░░░░░░░░░░   30%
 Notifications (dedup, digest, SMS)         ██████████░░░░░░░░░░   50%
 Testing & DevOps (CI, Docker, coverage)    ██████░░░░░░░░░░░░░░   30%
```

### What "core complete" means in practice

Three people, three months, one working system:
- A **live daily pipeline** that ingests 9 tickers and macro data every weekday evening
- A **Snowflake warehouse** with 7 signal views (returns, volatility, Z-scores, Fed rate, CPI, yield curve, summary)
- An **Airflow DAG** that orchestrates ingestion → signals → anomaly detection → notifications
- A **Streamlit dashboard** with four pages and AI-powered chat via Snowflake Cortex
- **Slack and email alerts** when anomalies are detected, with fail-open design
- **69 unit tests**, all passing without a live database connection
- Engineering patterns used in real production teams: circuit breakers, fallback chains, idempotent writes, pipeline observability

### What's left and what it takes

The remaining directions fall into three natural groups based on how much new ground they cover.

**Quick wins — no new tools needed**
These build entirely on what's already in place. Anyone comfortable with the existing codebase can pick one up independently.
- *Extend data sources* — add FRED (Federal Reserve's free API with 800,000+ economic series like GDP, housing, consumer sentiment) and SEC EDGAR (free public company filings) as new producers. The `BaseProducer` framework already handles retry and fallback; it's just a matter of writing a new subclass.
- *Enrich the signal library* — add RSI (momentum), moving average crossover (trend direction), sector rotation (which industries are leading), drawdown (how far a stock is from its peak). All pure SQL views on top of data we already store.
- *Polish the dashboard* — add ticker comparison, CSV export, a watchlist editor, and an alert history page. Pure Streamlit additions, no backend changes.
- *Harden the pipeline* — add a data quality check task in Airflow (assert row counts and no null prices before signals refresh), set up GitHub Actions so tests run automatically on every pull request.

**Medium lift — one new tool to learn**
Each of these introduces one new concept or tool that requires some ramp-up but pays off quickly.
- *Activate Kafka streaming* (Phase 2) — the code is fully written and the Docker setup exists. The main work is switching the DAG from "write directly to Snowflake" to "publish to Kafka, consume into Snowflake," and verifying message flow end-to-end. Introduces the concept of decoupled producers and consumers.
- *Adopt dbt for SQL transforms* — replace the hand-written `signals/*.sql` files with dbt models. dbt adds lineage graphs, auto-generated documentation, and `dbt test` assertions on top of the same SQL logic. It's the industry standard for managing SQL pipelines and directly relevant to CSE-5114.
- *Deepen Airflow usage* — add a weekly summary DAG, move Snowflake credentials into Airflow Connections (the production-standard way), and enable backfill so we can populate historical data. Each sub-task is independent.

**Significant investment — steep learning curve**
These are genuinely new engineering territory. They're the right choice if the goal is hands-on experience with Spark or Flink for class, not because the pipeline needs them for performance at our current data scale.
- *Apache Spark for batch signal computation* — re-implement rolling volatility and Z-scores as a PySpark job that reads from `RAW_STOCK_PRICES` and writes enriched results back to Snowflake. Teaches Spark DataFrames, window functions, and reading/writing Snowflake from Spark. Note: this replaces work Snowflake SQL already does well — the value is the learning experience.
- *Apache Flink for real-time streaming* — build a Flink job that consumes from the Kafka `raw.stock.prices` topic and computes a rolling Z-score in near-real-time, emitting intraday anomaly alerts rather than waiting until 6 PM. This requires a genuine real-time data feed — Yahoo Finance has a 15-minute delay and cannot feed Flink. The cleanest free path is to use a **crypto exchange WebSocket** (Binance or Coinbase offer unlimited free real-time streams, no account needed), since the data format is identical to stocks. Introduces event-time processing, watermarks, and stateful streaming operators.

The takeaway: most of what's left is **low-effort and free**, and the two heavyweight items (Spark, Flink) are worth doing specifically to practice those tools in a real context — not because the pipeline is slow or broken without them.

---

### How to split this across three people

A clean way to divide the work is by **layer** — each person owns one layer of the system end-to-end. This minimizes blocking (you're rarely waiting on each other), gives each person a coherent story to tell about what they built, and ensures all three class tools (Spark, Flink, dbt) are covered across the group.

---

#### Person A — Data Layer: *Richer Inputs, Better Transforms*
*Own what goes into the system and how it gets shaped.*

The data layer is about expanding where our data comes from and how cleanly it flows into Snowflake. Right now we have Yahoo Finance and Snowflake Marketplace. There's a much richer world of free data waiting — the Federal Reserve's FRED API alone has 800,000+ economic time series. Person A would plug those sources in, enrich the signal library with new SQL views, and adopt **dbt** to make the entire `signals/` folder properly documented, tested, and maintainable. This is the most SQL-and-Python-heavy role, with no infrastructure changes needed.

| Task | What it involves |
|---|---|
| FRED API producer | New `FredProducer(BaseProducer)` subclass — fetches GDP, housing starts, consumer sentiment, inflation expectations via the free FRED API |
| SEC EDGAR producer | Pull quarterly/annual filing text and feed summaries into the Cortex LLM for earnings context |
| New SQL signals | Add RSI (momentum), moving average crossover (trend direction), sector rotation (which industries lead/lag), drawdown (distance from 52-week high) as SQL views in `signals/` |
| dbt migration | Replace hand-written `signals/*.sql` with dbt models — adds lineage graphs, auto docs, `dbt test` data quality assertions |

**What you'd learn:** FRED/EDGAR API integration, SQL window functions for financial signals, dbt (industry-standard SQL transformation tool).

---

#### Person B — Pipeline Layer: *Reliable, Scalable Orchestration*
*Own how data moves from ingestion through to Snowflake.*

The pipeline layer is about making data movement more robust and scalable. The biggest item here is activating Kafka — the code already exists, it just needs to be switched on and verified end-to-end. From there, Person B would deepen the Airflow setup (a weekly summary DAG, data quality checks before signals refresh, backfill for historical data) and take on **Apache Spark** as the heavyweight class exercise — re-implementing signal computation as a distributed batch job.

| Task | What it involves |
|---|---|
| Activate Kafka (Phase 2) | Switch the DAG from direct Snowflake writes to Kafka publish → consumer → Snowflake. Verify message flow in Kafka UI |
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
| Apache Flink streaming job | Consume from Kafka `raw.stock.prices`, compute a rolling Z-score in near-real time, emit intraday anomaly events. Use Binance/Coinbase free WebSocket as the real-time data source |

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
| **Snowflake** | Central data warehouse. Stores raw ingested data, hosts signal computation as SQL views, and powers the dashboard queries |
| **Kafka** | Fully scaffolded for Phase 2 streaming. The producers and consumer are written; Kafka just needs to be switched on |

### What the pipeline does today

Every weekday evening, the Airflow DAG:
1. Fetches stock prices for 9 tickers (AAPL, MSFT, NVDA, TSLA, etc.) from Yahoo Finance
2. Fetches macro indicators (Fed Funds Rate, CPI) from Snowflake Marketplace
3. Runs all signal views in Snowflake (returns, volatility, Z-scores, yield curve)
4. Checks for anomalies — any ticker with a Z-score above 2.0
5. Sends alerts to Slack and/or email if anomalies are found
6. Logs the run to `PIPELINE_RUN_LOG` for observability

The Streamlit dashboard has four pages: a Chat page (AI-powered Q&A via Snowflake Cortex), Stock Deep Dive, Macro Overlay, and Pipeline Health.

### Engineering patterns we applied

- **Circuit breaker** on each data producer — if a source fails repeatedly, it pauses automatically rather than slowing the whole pipeline
- **Fallback chain** — producers are tried in priority order; if Yahoo Finance is down, the system can fall back to another source
- **Idempotent writes** via `MERGE INTO` — safe to re-run the pipeline without creating duplicate rows
- **Fail-open notifications** — if Slack is down, email still goes out; one broken channel never blocks the others
- **69 unit tests** covering every layer, all passing without a live Snowflake connection

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

Right now the pipeline writes directly from the producer to Snowflake. The more scalable pattern — and the one Kafka is designed for — is to decouple the producer from the writer. The producer just publishes a message; a separate consumer handles the Snowflake write. This makes the pipeline more resilient (the producer doesn't care if Snowflake is briefly down) and opens the door to multiple consumers reading the same stream.

- [ ] `FREE` Uncomment `kafka-python` in `requirements.txt` and bring up the Kafka stack with `docker compose -f docker-compose.kafka.yml up -d`
- [ ] `FREE` Switch `_ingest_prices()` in the DAG to use `fetch_and_publish_to_kafka` instead of `fetch_and_write_to_snowflake`
- [ ] `FREE` Add `SnowflakePricesConsumer` as a service in `docker-compose.kafka.yml` so it runs alongside Kafka
- [ ] `FREE` Add a dead-letter topic (`raw.stock.prices.dlq`) for messages that fail to write after N retries
- [ ] `FREE` Surface Kafka consumer lag on the Pipeline Health dashboard page

---

### Direction 2 — Richer Data Sources
*More data means richer signals. All the best options here are free.*

The most exciting opportunity is **FRED** — the Federal Reserve's public data API with 800,000+ economic time series (GDP, housing starts, consumer confidence, inflation expectations). It's completely free, requires no credit card, and would let us move beyond what Snowflake Marketplace licenses. SEC EDGAR is similarly compelling for earnings-season analysis — full 10-K and 10-Q filings, no API key required.

- [ ] `FREE` **FRED API producer** — swap the Snowflake Marketplace macro fetch for direct FRED API calls. Unlocks GDP, housing starts, consumer sentiment, and much more. Free key at fred.stlouisfed.org
- [ ] `FREE` **SEC EDGAR producer** — pull quarterly and annual filings text via the public EDGAR API and feed summaries into the Cortex LLM for earnings context
- [ ] `FREE TIER` **AlphaVantage fallback** — 25 free calls/day; useful as a priority-2 fallback behind Yahoo Finance, especially for intraday data. Free key at alphavantage.co
- [ ] `FREE TIER` **News sentiment** — fetch headlines from NewsAPI (100 free/day) or Finnhub (60 calls/min free) and score sentiment using Snowflake Cortex. Store in a `RAW_NEWS_SENTIMENT` table and add a sentiment signal view
- [ ] `PAID` **Polygon.io** — real-time quotes and options flow data; no meaningful free tier for historical OHLCV, but worth knowing about for when the project scales

---

### Direction 3 — New Signals & Analytics
*Pure SQL views in Snowflake — no new infrastructure, no extra cost.*

All of these build on data we already have. They would make the anomaly detection richer and give the AI more context to explain what it's seeing.

- [ ] `FREE` **RSI (Relative Strength Index)** — a classic momentum indicator; flag tickers above 70 (overbought) or below 30 (oversold) using SQL window functions
- [ ] `FREE` **Moving average crossover** — detect golden cross (20-day MA crosses above 50-day MA) and death cross events; historically meaningful trend signals
- [ ] `FREE` **Sector rotation** — compare sector ETF returns (XLK tech, XLF finance, XLE energy, etc.) to see which sectors are leading vs. lagging the S&P 500
- [ ] `FREE` **Correlation matrix** — compute pairwise return correlations across the watchlist to surface which stocks move together and which are truly independent
- [ ] `FREE` **Drawdown tracker** — how far each ticker is from its 52-week high; a simple but useful risk metric
- [ ] `FREE` **Earnings calendar** — mark earnings report dates so the anomaly detector can distinguish "stock moved because earnings" from a genuinely unexplained anomaly

---

### Direction 4 — Spark, Flink, and dbt
*The tools we're learning in class, applied to a real problem — but let's be honest about when each one makes sense.*

This is probably the most directly relevant direction for CSE-5114, so it's worth thinking carefully about *why* we'd use each tool rather than just adding them for the sake of it.

**An important insight about Spark and app performance:**
We noticed the dashboard can feel slow to load, and it might be tempting to think "let's add Spark to speed it up." But Spark would actually do nothing here — the slowness isn't a computation problem. Our bottleneck is Snowflake's warehouse cold start (it suspends after a few minutes of inactivity, and the first query waits 15–30 seconds for it to wake up), plus small inefficiencies in how the app manages its database connection. We've already fixed those in the code — caching the connection with `st.cache_resource`, removing an unnecessary `SELECT 1` round-trip before every query, and moving cached query functions to module level so they persist across page switches. The app should feel noticeably snappier now.

So when would Spark and Flink *actually* make sense for us?

- **Spark** becomes worth it when our data volume grows large enough that Snowflake SQL starts taking too long — think 500+ tickers, tick-by-tick intraday data (millions of rows per day), or training ML models on years of price history. At our current scale of 9 tickers with daily OHLCV data, Snowflake handles the computation in milliseconds. The right reason to try Spark now is to *learn it*, not because we need it.

- **Flink** becomes worth it when we want *intraday* alerts instead of end-of-day ones — detecting "NVDA is up 4% in the last 20 minutes" in near-real-time rather than at 6 PM. But Flink needs a real-time data feed to consume from. Yahoo Finance (our current source) has a 15-minute delay and doesn't stream. To use Flink meaningfully, we'd need a streaming data source — the best free option for learning is a **crypto exchange WebSocket** (Binance or Coinbase offer free, unlimited real-time price streams with no API key). The data format is identical to stocks, so a Flink job written for BTC/ETH prices works the same way for stocks once you have a paid feed.

- **dbt** is the most immediately useful of the three. It doesn't change what our SQL does — it just makes the `signals/*.sql` files dramatically more maintainable: auto-generated documentation, lineage graphs showing how views depend on each other, and `dbt test` for catching bad data before it reaches the dashboard. This is the industry standard and the lowest-effort way to level up the pipeline.

- [ ] `FREE` **dbt models** — replace the hand-written `signals/*.sql` files with dbt. dbt Core is free and open source. It adds lineage graphs, auto-generated documentation, and `dbt test` for data quality assertions. This is the industry standard for SQL-based transformation and directly relevant to what data engineers do day to day
- [ ] `FREE` **Apache Spark batch job** — re-implement rolling volatility and Z-score computation as a PySpark job that reads from `RAW_STOCK_PRICES` and writes to a `SIGNALS_STOCK` table. Good hands-on practice with Spark DataFrames and window functions. The right reason to do this is learning, not performance — our current Snowflake views are already fast enough
- [ ] `FREE` **Apache Flink streaming job** — consume from the Kafka `raw.stock.prices` topic in real time and compute a rolling Z-score as messages arrive. To make this genuinely real-time, pair it with a free crypto WebSocket feed (Binance/Coinbase) as the data source. Demonstrates Flink's event-time processing and watermarks, and produces near-real-time anomaly alerts rather than end-of-day ones

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
