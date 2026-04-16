# MarketLens — Data Sources

## Where the data comes from

| Data | Source | Provider | Cost |
|---|---|---|---|
| **Stock prices** (OHLCV — AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, SPY, QQQ) | Snowflake Marketplace — `STOCK_PRICE_TIMESERIES` | Snowflake `SNOWFLAKE_PUBLIC_DATA_FREE` share | Free |
| **Fed Funds Rate** (EFFR) | Snowflake Marketplace — `FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES` | Snowflake `SNOWFLAKE_PUBLIC_DATA_FREE` share | Free |
| **CPI** (All Items, seasonally adjusted, monthly) | Snowflake Marketplace — `BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES` | Snowflake `SNOWFLAKE_PUBLIC_DATA_FREE` share | Free |
| **SEC filings** (10-K / 10-Q narrative text) | Snowflake Marketplace — `SEC_CORPORATE_REPORT_TEXT_ATTRIBUTES` | Snowflake `SNOWFLAKE_PUBLIC_DATA_FREE` share | Free |
| **10Y Treasury yield** (DGS10, daily) | FRED API — St. Louis Fed | `ingestion/fred_producer.py` | Free (requires `FRED_API_KEY`) |
| **Real GDP** (GDPC1, quarterly SAAR) | FRED API — St. Louis Fed | `ingestion/fred_producer.py` | Free (requires `FRED_API_KEY`) |
| **Housing Starts** (HOUST, monthly SAAR) | FRED API — St. Louis Fed | `ingestion/fred_producer.py` | Free (requires `FRED_API_KEY`) |
| **Consumer Sentiment** (UMCSENT, monthly) | FRED API — St. Louis Fed | `ingestion/fred_producer.py` | Free (requires `FRED_API_KEY`) |
| **10Y Breakeven Inflation** (T10YIE, daily) | FRED API — St. Louis Fed | `ingestion/fred_producer.py` | Free (requires `FRED_API_KEY`) |
| **10Y Treasury yield** (alt, Z.1 series) | Snowflake Marketplace — `FEDERAL_RESERVE_TIMESERIES_PIT` | Snowflake `SNOWFLAKE_PUBLIC_DATA_PAID` share | **Paid — not used** |
| **Unemployment rate** | Snowflake Marketplace — `BUREAU_OF_LABOR_STATISTICS_EMPLOYMENT_TIMESERIES` | Snowflake `SNOWFLAKE_PUBLIC_DATA_PAID` share | **Paid — not used** |

## How to get a FRED API key

FRED keys are free. Register at: https://fred.stlouisfed.org/docs/api/api_key.html

Once you have the key, add it to `.env`:

```
FRED_API_KEY=your_key_here
```

Then re-run ingestion and rebuild dbt models:

```bash
source .venv/bin/activate
python run_fred_ingest.py
cd dbt && dbt build --profiles-dir . && cd ..
```

## Data flow summary

```
Snowflake Marketplace (free)         FRED API (free, key required)
        │                                        │
        ▼                                        ▼
RAW_STOCK_PRICES                     RAW_FRED_INDICATORS
RAW_MACRO_INDICATORS                 (DGS10, GDPC1, HOUST, UMCSENT, T10YIE)
SEC_FILING_SUMMARIES
        │                                        │
        └──────────────────┬─────────────────────┘
                           ▼
                    dbt staging models
                    (V_STOCK_PRICES, V_FED_FUNDS_RATE, V_CPI,
                     V_10Y_TREASURY_FRED, V_FRED_GDP, V_FRED_HOUSING,
                     V_FRED_SENTIMENT, V_FRED_INFLATION_EXPECTATIONS)
                           │
                           ▼
                    dbt mart models
                    (V_ANOMALY_SCORES, V_RSI_14, V_MA_CROSSOVER,
                     V_DRAWDOWN, V_SECTOR_ROTATION, V_DAILY_RETURNS,
                     V_FED_RATE_CHANGES, V_CPI_CHANGES, V_YIELD_CURVE,
                     V_YIELD_CURVE_SIGNALS, V_GDP_CHANGES,
                     V_SENTIMENT_CHANGES, V_SEC_NARRATIVES)
                           │
                           ▼
                    V_SIGNAL_SUMMARY  ◄──── all signals unified here
                           │
                           ▼
                    app/app.py (Streamlit dashboard)
```
