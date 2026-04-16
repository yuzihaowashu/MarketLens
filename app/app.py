import streamlit as st
import pandas as pd
import datetime
import html
import re
import time
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from snowflake_client import run_query, run_query_single
from config import LLM_MODEL, WATCHLIST_TICKERS, TICKER_NAMES, PULSE_TICKERS

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────
st.set_page_config(page_title='MarketLens', page_icon='🔍', layout='wide',
                   initial_sidebar_state='expanded')

TICKERS = WATCHLIST_TICKERS
MAX_HISTORY_TURNS = 3

LEVELS = {
    'curious': {
        'label': "Just Curious",
        'icon': '🌱',
        'tagline': "I have zero finance background — explain like I'm five!",
        'system_prompt': (
            "You are MarketLens, a patient and friendly assistant explaining financial markets "
            "to someone with absolutely no finance background. Use everyday analogies (pizza, "
            "Netflix, college life). Avoid ALL jargon — if you must use a term, define it "
            "immediately in parentheses. Keep answers to 3-4 short sentences. Never give "
            "investment advice or predictions."
        ),
        'suggestions': [
            ("🍎 What is Apple stock?", "What is Apple stock and why do people care about it?"),
            ("📉 Market crash?", "What does it mean when people say the market is crashing?"),
            ("💰 What is inflation?", "What is inflation and how does it affect me as a student?"),
            ("🏦 Interest rates?", "What are interest rates and why are they in the news?"),
        ],
        'show_market_pulse': False,
        'show_signals': False,
    },
    'intermediate': {
        'label': "Know the Basics",
        'icon': '📊',
        'tagline': "I understand stocks & bonds — show me what's interesting.",
        'system_prompt': (
            "You are MarketLens, a helpful market analyst assistant for someone who understands "
            "basic financial concepts (stocks, bonds, interest rates, inflation). You can use "
            "standard terms but briefly clarify advanced ones. Give concise, informative answers "
            "(3-5 sentences). Include relevant numbers when available. Never give investment "
            "advice or predictions."
        ),
        'suggestions': [
            ("🍎 Apple recently?", "What's Apple's stock price doing recently?"),
            ("⚡ Any anomalies?", "Are there any unusual market movements lately?"),
            ("🏦 Fed rate impact?", "What's happening with the Fed funds rate and how might it affect equities?"),
            ("📊 Volatility check", "Which stocks have the highest volatility right now?"),
        ],
        'show_market_pulse': True,
        'show_signals': True,
    },
    'analyst': {
        'label': "Financial Analyst",
        'icon': '🎯',
        'tagline': "Give me the data, z-scores, and signals — I can handle it.",
        'system_prompt': (
            "You are MarketLens, a concise data-driven assistant for a financial analyst. "
            "Use precise technical language freely (z-scores, basis points, vol surface, etc.). "
            "Reference specific metrics, dates, and magnitudes from the provided context only — "
            "never use placeholders like [date] or [ticker]; each signal line begins with an ISO date "
            "(YYYY-MM-DD) — cite those dates verbatim when you mention timing. "
            "Be direct and quantitative. 3-5 sentences. Never give investment advice."
        ),
        'suggestions': [
            ("📈 Top anomaly signals", "Show me the highest-salience anomaly signals from the past week."),
            ("📉 Vol regime shift?", "Has rolling 20-day volatility shifted regime for any tickers recently?"),
            ("🏦 Rate change impact", "What was the magnitude of the most recent Fed funds rate change and its z-score?"),
            ("📊 Cross-asset check", "Compare recent anomaly scores across AAPL, TSLA, and NVDA."),
        ],
        'show_market_pulse': True,
        'show_signals': True,
    },
}


# ─────────────────────────────────────────────────────────────────────
# Global CSS  (chat page styles — landing page injects its own block)
# ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
/*
 * Do NOT use `[class*="st-"] { font-family: Inter }` — it overrides Streamlit header/sidebar
 * Material Symbols icons; Inter has no those ligatures, so you see literal "keyboard_double_arrow_right".
 */
html, body { font-family: 'Inter', sans-serif; }
.stApp .main .block-container { font-family: 'Inter', sans-serif; }

.topbar {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 16px; padding: 1rem 2rem; color: white;
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 1rem;
}
.topbar-left { font-size: 1.2rem; font-weight: 600; }
.topbar-right { font-size: 0.9rem; opacity: 0.8; }

.insight-card {
    border: 1px solid rgba(102,126,234,0.25);
    border-radius: 14px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.5rem;
    font-size: 0.88rem;
    line-height: 1.55;
    background: linear-gradient(135deg, rgba(102,126,234,0.06), rgba(118,75,162,0.06));
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────
for _key, _default in [
    ('level', None), ('messages', []),
    ('pending_q', None), ('_transitioning', False),
    ('page', 'chat'), ('admin_mode', False),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default


# ─────────────────────────────────────────────────────────────────────
# Route: handle ?level=xxx  (landing card clicks use <a> tags)
# ─────────────────────────────────────────────────────────────────────
_qp = st.query_params
if 'level' in _qp:
    chosen = _qp.get('level', '')
    if chosen in LEVELS:
        st.session_state.level = chosen
        st.session_state.messages = []
    st.query_params.clear()
    st.rerun()

# Clean transition spinner (used when going back to landing from chat)
if st.session_state._transitioning:
    st.session_state._transitioning = False
    with st.spinner("Loading..."):
        time.sleep(0.3)
    st.rerun()


# ─────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def get_top_signals(n=5):
    cols, rows = run_query(
        "SELECT DATE, SIGNAL_TYPE, ENTITY, MAGNITUDE, SALIENCE_SCORE, SUMMARY "
        "FROM V_SIGNAL_SUMMARY ORDER BY DATE DESC, ABS(SALIENCE_SCORE) DESC "
        "LIMIT %s",
        (n,),
    )
    return pd.DataFrame(rows, columns=cols)


def _format_signal_context_row(row):
    """Attach a concrete DATE to each signal line for LLM context (avoids [date] placeholders)."""
    d = row['DATE']
    try:
        ts = pd.to_datetime(d)
        ds = 'unknown date' if pd.isna(ts) else ts.strftime('%Y-%m-%d')
    except (TypeError, ValueError, OverflowError):
        ds = str(d) if d is not None and str(d) != 'NaT' else 'unknown date'
    return f"{ds}: {row['SUMMARY']}"


def _format_insights_for_display(text: str) -> str:
    """Escape HTML; break inline • bullets onto separate lines (LLM often emits one long line)."""
    if not text:
        return ""
    t = html.escape(text.strip())
    t = t.replace('\n', '<br>')
    t = re.sub(r'\s+•\s+', '<br>• ', t)
    t = re.sub(r'^(?:<br>)+', '', t)
    return t


@st.cache_data(ttl=600, show_spinner=False)
def get_price_data(ticker, days=30):
    cols, rows = run_query(
        "SELECT DATE, CLOSE_PRICE FROM V_STOCK_PRICES "
        "WHERE TICKER = %s AND CLOSE_PRICE IS NOT NULL "
        "ORDER BY DATE DESC LIMIT %s",
        (ticker, days),
    )
    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values('DATE')
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_latest_price(ticker):
    cols, rows = run_query(
        "SELECT DATE, CLOSE_PRICE FROM V_STOCK_PRICES "
        "WHERE TICKER = %s AND CLOSE_PRICE IS NOT NULL "
        "ORDER BY DATE DESC LIMIT 2",
        (ticker,),
    )
    if len(rows) >= 2:
        latest, prev = rows[0], rows[1]
        return latest[1], (latest[1] - prev[1]) / prev[1] * 100, latest[0]
    return None, None, None


@st.cache_data(ttl=600, show_spinner=False)
def get_data_freshness():
    return run_query_single(
        "SELECT MAX(DATE) FROM V_STOCK_PRICES WHERE CLOSE_PRICE IS NOT NULL"
    )


@st.cache_data(ttl=900, show_spinner=False)
def get_market_insights():
    """LLM-generated news / trends from recent signals."""
    sdf = get_top_signals(6)
    if sdf.empty:
        return None
    signal_text = '\n'.join(f"- {_format_signal_context_row(row)}" for _, row in sdf.iterrows())
    prompt = (
        "You are a concise financial news writer. Based on these market signals, "
        "write 4-5 brief news-style bullet points. "
        "Cover anomalies, notable price moves, macro trends, and patterns. "
        "Each bullet: one sentence with specific numbers. "
        "Formatting: start every bullet with • (bullet) and a space; put each bullet on its own line "
        "(newline after each bullet). Never put two bullets on the same line. "
        "Do NOT give investment advice.\n\n"
        f"Signals:\n{signal_text}"
    )
    return run_query_single(
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', %s)", (prompt,)
    )


@st.cache_data(ttl=600, show_spinner=False)
def get_signal_details(signal_type, entity, signal_date):
    if signal_type == 'STOCK_ANOMALY':
        cols, rows = run_query(
            "SELECT TICKER, DATE, DAILY_RETURN, AVG_RETURN_20D, "
            "VOLATILITY_20D, Z_SCORE "
            "FROM V_ANOMALY_SCORES WHERE TICKER = %s AND DATE = %s",
            (entity, signal_date),
        )
        if rows:
            r = rows[0]
            return {
                'Ticker': r[0], 'Date': str(r[1]),
                'Daily Return': f"{r[2] * 100:.2f}%",
                '20-Day Avg Return': f"{r[3] * 100:.3f}%",
                '20-Day Volatility': f"{r[4] * 100:.3f}%",
                'Z-Score': f"{r[5]:.2f}",
            }
    elif signal_type == 'FED_RATE_CHANGE':
        cols, rows = run_query(
            "SELECT DATE, FED_FUNDS_RATE, PREV_RATE, RATE_CHANGE "
            "FROM V_FED_RATE_CHANGES WHERE DATE = %s",
            (signal_date,),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                'Current Rate': f"{r[1] * 100:.2f}%",
                'Previous Rate': f"{r[2] * 100:.2f}%" if r[2] else "N/A",
                'Change (bps)': f"{r[3] * 10000:.1f}",
            }
    elif signal_type == 'CPI_CHANGE':
        cols, rows = run_query(
            "SELECT DATE, CPI_INDEX, PREV_CPI, CPI_MOM_CHANGE "
            "FROM V_CPI_CHANGES WHERE DATE = %s",
            (signal_date,),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                'CPI Index': f"{r[1]:.2f}",
                'Previous CPI': f"{r[2]:.2f}" if r[2] else "N/A",
                'MoM Change': f"{r[3] * 100:.3f}%",
            }
    elif signal_type == 'RSI_EXTREME':
        cols, rows = run_query(
            "SELECT TICKER, DATE, RSI_14, RSI_STATE "
            "FROM V_RSI_14 WHERE TICKER = %s AND DATE = %s",
            (entity, signal_date),
        )
        if rows:
            r = rows[0]
            return {
                'Ticker': r[0], 'Date': str(r[1]),
                'RSI(14)': f"{r[2]:.1f}",
                'State': r[3],
            }
    elif signal_type == 'MA_CROSSOVER':
        cols, rows = run_query(
            "SELECT TICKER, DATE, SMA_50, SMA_200, CROSSOVER_EVENT "
            "FROM V_MA_CROSSOVER WHERE TICKER = %s AND DATE = %s",
            (entity, signal_date),
        )
        if rows:
            r = rows[0]
            return {
                'Ticker': r[0], 'Date': str(r[1]),
                'SMA 50': f"${r[2]:.2f}",
                'SMA 200': f"${r[3]:.2f}",
                'Event': r[4],
            }
    elif signal_type == 'DRAWDOWN':
        cols, rows = run_query(
            "SELECT TICKER, DATE, CLOSE_PRICE, HIGH_52W, DRAWDOWN_PCT, DRAWDOWN_STATE "
            "FROM V_DRAWDOWN WHERE TICKER = %s AND DATE = %s",
            (entity, signal_date),
        )
        if rows:
            r = rows[0]
            return {
                'Ticker': r[0], 'Date': str(r[1]),
                'Close': f"${r[2]:.2f}",
                '52W High': f"${r[3]:.2f}",
                'Drawdown': f"{r[4] * 100:.1f}%",
                'State': r[5],
            }
    elif signal_type == 'SECTOR_ROTATION':
        cols, rows = run_query(
            "SELECT DATE, SECTOR, AVG_20D_RETURN, SECTOR_RANK "
            "FROM V_SECTOR_ROTATION WHERE SECTOR = %s AND DATE = %s",
            (entity, signal_date),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                'Sector': r[1],
                '20D Avg Return': f"{r[2] * 100:.2f}%",
                'Rank': str(r[3]),
            }
    elif signal_type == 'YIELD_CURVE':
        cols, rows = run_query(
            "SELECT DATE, TREASURY_10Y, FED_FUNDS_RATE, CURVE_SPREAD, IS_INVERTED "
            "FROM V_YIELD_CURVE WHERE DATE = %s",
            (signal_date,),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                '10Y Treasury': f"{r[1] * 100:.2f}%",
                'Fed Funds Rate': f"{r[2] * 100:.2f}%",
                'Spread': f"{r[3] * 100:.2f}%",
                'Inverted': str(r[4]),
            }
    elif signal_type == 'GDP_CONTRACTION':
        cols, rows = run_query(
            "SELECT DATE, GDP_REAL_BILLIONS, QOQ_GROWTH_PCT "
            "FROM V_GDP_CHANGES WHERE DATE = %s",
            (signal_date,),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                'Real GDP': f"${r[1]:,.0f}B",
                'QoQ Growth': f"{r[2]:+.2f}%",
            }
    elif signal_type == 'SENTIMENT_SHIFT':
        cols, rows = run_query(
            "SELECT DATE, SENTIMENT_INDEX, MOM_CHANGE, SENTIMENT_EVENT "
            "FROM V_SENTIMENT_CHANGES WHERE DATE = %s",
            (signal_date,),
        )
        if rows:
            r = rows[0]
            return {
                'Date': str(r[0]),
                'Sentiment Index': f"{r[1]:.1f}",
                'MoM Change': f"{r[2]:+.1f} pts",
                'Event': r[3],
            }
    return None


# ─────────────────────────────────────────────────────────────────────
# Module-level cached queries for Deep Dive, Macro, and Pipeline pages.
# Defined here (not inside page blocks) so Streamlit can reuse the cache
# across reruns without re-registering the function on every interaction.
# ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def get_anomaly_data(ticker, days):
    cols, rows = run_query(
        'SELECT DATE, DAILY_RETURN, AVG_RETURN_20D, VOLATILITY_20D, Z_SCORE, IS_ANOMALY '
        'FROM V_ANOMALY_SCORES WHERE TICKER = %s AND DATE IS NOT NULL '
        'ORDER BY DATE DESC LIMIT %s',
        (ticker, days),
    )
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=1800, show_spinner=False)
def get_fed_rate_data(days=365):
    cols, rows = run_query(
        'SELECT DATE, FED_FUNDS_RATE, RATE_CHANGE '
        'FROM V_FED_RATE_CHANGES WHERE FED_FUNDS_RATE IS NOT NULL '
        'ORDER BY DATE DESC LIMIT %s',
        (days,),
    )
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=1800, show_spinner=False)
def get_cpi_data(months=24):
    cols, rows = run_query(
        'SELECT DATE, CPI_INDEX, CPI_MOM_CHANGE '
        'FROM V_CPI_CHANGES WHERE CPI_INDEX IS NOT NULL '
        'ORDER BY DATE DESC LIMIT %s',
        (months,),
    )
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=1800, show_spinner=False)
def get_yield_curve_data():
    try:
        cols, rows = run_query(
            'SELECT DATE, TREASURY_10Y, FED_FUNDS_RATE, CURVE_SPREAD, IS_INVERTED '
            'FROM V_YIELD_CURVE ORDER BY DATE DESC LIMIT 40'
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_gdp_data():
    try:
        cols, rows = run_query(
            'SELECT DATE, GDP_REAL_BILLIONS, QOQ_GROWTH_PCT, IS_CONTRACTION '
            'FROM V_GDP_CHANGES WHERE GDP_REAL_BILLIONS IS NOT NULL '
            'ORDER BY DATE DESC LIMIT 40'
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_housing_data():
    try:
        cols, rows = run_query(
            'SELECT DATE, HOUSING_STARTS_K '
            'FROM V_FRED_HOUSING WHERE HOUSING_STARTS_K IS NOT NULL '
            'ORDER BY DATE DESC LIMIT 60'
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_sentiment_data():
    try:
        cols, rows = run_query(
            'SELECT DATE, SENTIMENT_INDEX, MOM_CHANGE, SENTIMENT_EVENT '
            'FROM V_SENTIMENT_CHANGES WHERE SENTIMENT_INDEX IS NOT NULL '
            'ORDER BY DATE DESC LIMIT 60'
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def get_inflation_expectations_data():
    try:
        cols, rows = run_query(
            'SELECT DATE, INFLATION_EXPECTATION_PCT '
            'FROM V_FRED_INFLATION_EXPECTATIONS WHERE INFLATION_EXPECTATION_PCT IS NOT NULL '
            'ORDER BY DATE DESC LIMIT 250'
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5, show_spinner=False)
def get_kafka_consumer_lag() -> dict:
    """Return {group: lag_int_or_None} for all four streaming consumer groups."""
    try:
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient
        admin  = KafkaAdminClient(bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
                                  client_id='marketlens-health',
                                  request_timeout_ms=3000)
        helper = KafkaConsumer(bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
                               request_timeout_ms=3000)
        groups = ['sf-writer', 'anomaly-check', 'notifier', 'dashboard']
        result = {}
        for group in groups:
            try:
                offsets = admin.list_consumer_group_offsets(group)
                if not offsets:
                    result[group] = None
                    continue
                end_offsets = helper.end_offsets(list(offsets.keys()))
                lag = sum(max(0, end_offsets.get(tp, meta.offset) - meta.offset)
                          for tp, meta in offsets.items())
                result[group] = lag
            except Exception:
                result[group] = None
        admin.close()
        helper.close()
        return result
    except Exception:
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_runs(limit=30):
    try:
        cols, rows = run_query(
            'SELECT RUN_ID, DAG_ID, TASK_ID, STATUS, ROW_COUNT, '
            'ERROR_MSG, STARTED_AT, COMPLETED_AT, '
            'DATEDIFF(\'second\', STARTED_AT, COMPLETED_AT) AS DURATION_SEC '
            'FROM SCORPION_DB.MARKETLENS.PIPELINE_RUN_LOG '
            'ORDER BY STARTED_AT DESC LIMIT %s',
            (limit,),
        )
        return pd.DataFrame(rows, columns=cols), None
    except Exception as _e:
        return pd.DataFrame(), str(_e)


@st.cache_data(ttl=600, show_spinner=False)
def get_signal_stats():
    """Total signal count for the stats ribbon."""
    try:
        _, r = run_query(
            'SELECT COUNT(*), COUNT(DISTINCT SIGNAL_TYPE), COUNT(DISTINCT ENTITY) '
            'FROM V_SIGNAL_SUMMARY'
        )
        if r:
            return {'total': int(r[0][0]), 'types': int(r[0][1]), 'entities': int(r[0][2])}
    except Exception:
        pass
    return {'total': 0, 'types': 0, 'entities': 0}


@st.cache_data(ttl=600, show_spinner=False)
def get_signal_heatmap_data():
    """Counts of ticker-level signals by type — used to draw the signal heatmap."""
    try:
        cols, rows = run_query(
            "SELECT ENTITY, SIGNAL_TYPE, COUNT(*) AS CNT "
            "FROM V_SIGNAL_SUMMARY "
            "WHERE SIGNAL_TYPE IN ('STOCK_ANOMALY','RSI_EXTREME','MA_CROSSOVER','DRAWDOWN') "
            "GROUP BY ENTITY, SIGNAL_TYPE"
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=900, show_spinner=False)
def get_story_of_day():
    """One plain-English paragraph summarising the market for Just Curious users."""
    try:
        sdf = get_top_signals(3)
        if sdf.empty:
            return None
        signal_text = '\n'.join(
            f"- {_format_signal_context_row(row)}" for _, row in sdf.iterrows()
        )
        prompt = (
            "You are a friendly market storyteller writing for someone with zero finance "
            "knowledge. Based on the signals below, write ONE engaging paragraph (3-4 sentences) "
            "explaining what's happening in the market today. Use everyday analogies. "
            "No jargon. Do NOT give investment advice.\n\n"
            f"Signals:\n{signal_text}"
        )
        return run_query_single(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', %s)", (prompt,)
        )
    except Exception:
        return None


@st.cache_data(ttl=600, show_spinner=False)
def get_sec_tone_breakdown():
    """Count of SEC filings by management tone — shown in sidebar."""
    try:
        cols, rows = run_query(
            "SELECT COALESCE(UPPER(MANAGEMENT_TONE), 'UNKNOWN') AS TONE, "
            "COUNT(*) AS CNT "
            "FROM V_SEC_NARRATIVES GROUP BY TONE ORDER BY CNT DESC"
        )
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def get_ticker_signal_summary(ticker: str) -> dict:
    """Latest RSI, MA crossover, and drawdown state for a ticker — used in Deep Dive header."""
    result: dict = {}
    try:
        _, r = run_query(
            "SELECT RSI_14, RSI_STATE FROM V_RSI_14 "
            "WHERE TICKER = %s AND DATE IS NOT NULL ORDER BY DATE DESC LIMIT 1",
            (ticker,),
        )
        if r:
            result['rsi'] = r[0][0]
            result['rsi_state'] = r[0][1]
    except Exception:
        pass
    try:
        _, r = run_query(
            "SELECT CROSSOVER_EVENT, SMA_50, SMA_200 FROM V_MA_CROSSOVER "
            "WHERE TICKER = %s AND DATE IS NOT NULL ORDER BY DATE DESC LIMIT 1",
            (ticker,),
        )
        if r:
            result['ma_event'] = r[0][0]
            result['sma50'] = r[0][1]
            result['sma200'] = r[0][2]
    except Exception:
        pass
    try:
        _, r = run_query(
            "SELECT DRAWDOWN_PCT, DRAWDOWN_STATE FROM V_DRAWDOWN "
            "WHERE TICKER = %s AND DATE IS NOT NULL ORDER BY DATE DESC LIMIT 1",
            (ticker,),
        )
        if r:
            result['drawdown_pct'] = r[0][0]
            result['drawdown_state'] = r[0][1]
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────────────────────────────
# Admin-mode tech note helper
# ─────────────────────────────────────────────────────────────────────
def _tech_note(title: str, body: str):
    """Render a tech-stack explainer banner when Admin Mode is on."""
    if not st.session_state.get('admin_mode', False):
        return
    # Escape HTML, then highlight backtick spans as inline code
    safe_body = html.escape(body)
    safe_body = re.sub(
        r'`([^`]+)`',
        r'<code style="background:rgba(102,126,234,0.22);padding:0 4px;border-radius:3px;'
        r'font-family:monospace;font-size:0.80rem;color:#a8c8ff;">\1</code>',
        safe_body,
    )
    st.markdown(
        f"""<div style="
            background: linear-gradient(135deg,rgba(15,15,35,0.97),rgba(20,10,45,0.97));
            border: 1.5px solid rgba(102,126,234,0.45);
            border-left: 4px solid #667eea;
            border-radius: 10px;
            padding: 0.7rem 1.1rem;
            margin: 0 0 1rem 0;
            font-size: 0.82rem;
            line-height: 1.65;
            color: #c8d0f8;
        ">
        <span style="color:#8fa4ff;font-weight:700;letter-spacing:0.03em;">
            ⚙ TECH STACK &mdash; {html.escape(title)}
        </span><br>
        {safe_body}
        </div>""",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────
# Stats ribbon renderer  (Analyst mode — top of every page)
# ─────────────────────────────────────────────────────────────────────
def _render_stats_ribbon():
    stats = get_signal_stats()
    items = [
        ('🎯', str(len(WATCHLIST_TICKERS)), 'Tickers'),
        ('📡', '10', 'Signal Types'),
        ('🏦', '7', 'Macro Indicators'),
        ('🔧', '13', 'dbt Models'),
        ('🔔', f"{stats['total']:,}" if stats['total'] else '—', 'Signals Detected'),
        ('⚡', 'Live', 'Kafka Stream'),
    ]
    _rcols = st.columns(len(items))
    for _c, (_icon, _val, _lbl) in zip(_rcols, items):
        _c.markdown(
            f"""<div style="text-align:center;padding:0.55rem 0.2rem;
                background:linear-gradient(135deg,rgba(102,126,234,0.10),rgba(118,75,162,0.10));
                border-radius:10px;border:1px solid rgba(102,126,234,0.20);margin-bottom:0.1rem;">
                <div style="font-size:1.3rem;line-height:1.2;">{_icon}</div>
                <div style="font-size:1.05rem;font-weight:700;color:#667eea;">{_val}</div>
                <div style="font-size:0.70rem;color:#444;margin-top:1px;">{_lbl}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    st.markdown('<div style="margin-bottom:0.6rem;"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Context builder
# ─────────────────────────────────────────────────────────────────────
def build_context(question):
    parts, q = [], question.lower()

    for t in TICKERS:
        if t.lower() in q or TICKER_NAMES.get(t, '').lower() in q:
            try:
                p, c, d = get_latest_price(t)
                if p:
                    parts.append(
                        f"{TICKER_NAMES.get(t, t)} ({t}) latest close: "
                        f"${p:.2f} on {d}, daily change: {c:+.2f}%"
                    )
            except Exception as e:
                logger.warning("Failed to fetch price for %s: %s", t, e)
            try:
                _, r = run_query(
                    "SELECT FORM_TYPE, FILING_DATE, MANAGEMENT_TONE, "
                    "REVENUE_NARRATIVE, GUIDANCE_NARRATIVE, RISK_NARRATIVE "
                    "FROM V_SEC_NARRATIVES WHERE TICKER = %s "
                    "ORDER BY FILING_DATE DESC LIMIT 1",
                    (t,),
                )
                if r:
                    form, fdate, tone, rev, guid, risk = r[0]
                    parts.append(
                        f"{t} latest {form} filed {fdate} (tone: {tone or 'n/a'}).\n"
                        f"  Revenue: {(rev or '')[:300]}\n"
                        f"  Guidance: {(guid or '')[:300]}\n"
                        f"  Risk: {(risk or '')[:300]}"
                    )
            except Exception as e:
                logger.warning("Failed to fetch SEC narrative for %s: %s", t, e)

    if any(kw in q for kw in ['anomal', 'unusual', 'weird', 'strange', 'signal']):
        try:
            adf = get_top_signals(5)
            if not adf.empty:
                lines = [f"  - {_format_signal_context_row(row)}" for _, row in adf.iterrows()]
                parts.append("Recent notable signals:\n" + '\n'.join(lines))
        except Exception as e:
            logger.warning("Failed to fetch signals: %s", e)

    if any(kw in q for kw in ['rate', 'interest', 'fed']):
        try:
            _, r = run_query(
                "SELECT DATE, FED_FUNDS_RATE FROM V_FED_FUNDS_RATE "
                "WHERE FED_FUNDS_RATE IS NOT NULL ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Latest Fed Funds Rate: {r[0][1] * 100:.2f}% as of {r[0][0]}"
                )
        except Exception as e:
            logger.warning("Failed to fetch Fed rate: %s", e)

    if any(kw in q for kw in ['volatil', 'vol ']):
        try:
            _, r = run_query(
                "SELECT TICKER, VOLATILITY_20D FROM V_ROLLING_VOLATILITY "
                "WHERE VOLATILITY_20D IS NOT NULL "
                "ORDER BY DATE DESC, VOLATILITY_20D DESC LIMIT 5"
            )
            if r:
                parts.append(
                    "Top volatile tickers: "
                    + ', '.join(f"{row[0]} (vol={row[1]:.4f})" for row in r)
                )
        except Exception as e:
            logger.warning("Failed to fetch volatility: %s", e)

    if any(kw in q for kw in ['cpi', 'inflation', 'price level']):
        try:
            _, r = run_query(
                "SELECT DATE, CPI_INDEX, CPI_MOM_CHANGE FROM V_CPI_CHANGES "
                "WHERE CPI_MOM_CHANGE IS NOT NULL ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Latest CPI: {r[0][1]:.2f} as of {r[0][0]}, "
                    f"month-over-month change: {r[0][2] * 100:.3f}%"
                )
        except Exception as e:
            logger.warning("Failed to fetch CPI: %s", e)

    if any(kw in q for kw in ['gdp', 'growth', 'recession', 'economy', 'economic']):
        try:
            _, r = run_query(
                "SELECT DATE, GDP_REAL_BILLIONS, QOQ_GROWTH_PCT, IS_CONTRACTION "
                "FROM V_GDP_CHANGES WHERE GDP_REAL_BILLIONS IS NOT NULL "
                "ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Latest Real GDP: ${r[0][1]:,.0f}B as of {r[0][0]}, "
                    f"QoQ growth: {r[0][2]:+.2f}%"
                    + (" (CONTRACTION)" if r[0][3] else "")
                )
        except Exception as e:
            logger.warning("Failed to fetch GDP: %s", e)

    if any(kw in q for kw in ['housing', 'real estate', 'construction', 'home']):
        try:
            _, r = run_query(
                "SELECT DATE, HOUSING_STARTS_K FROM V_FRED_HOUSING "
                "WHERE HOUSING_STARTS_K IS NOT NULL ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Latest Housing Starts: {r[0][1]:,.0f}K units (SAAR) as of {r[0][0]}"
                )
        except Exception as e:
            logger.warning("Failed to fetch housing starts: %s", e)

    if any(kw in q for kw in ['sentiment', 'consumer', 'confidence', 'umich']):
        try:
            _, r = run_query(
                "SELECT DATE, SENTIMENT_INDEX, MOM_CHANGE FROM V_SENTIMENT_CHANGES "
                "WHERE SENTIMENT_INDEX IS NOT NULL ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Latest Consumer Sentiment (UMich): {r[0][1]:.1f} as of {r[0][0]}, "
                    f"MoM change: {r[0][2]:+.1f} pts"
                )
        except Exception as e:
            logger.warning("Failed to fetch consumer sentiment: %s", e)

    if any(kw in q for kw in ['breakeven', 'inflation expectation', 'tips', 'treasury']):
        try:
            _, r = run_query(
                "SELECT DATE, INFLATION_EXPECTATION_PCT FROM V_FRED_INFLATION_EXPECTATIONS "
                "WHERE INFLATION_EXPECTATION_PCT IS NOT NULL ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"10Y Breakeven Inflation: {r[0][1] * 100:.2f}% as of {r[0][0]}"
                )
        except Exception as e:
            logger.warning("Failed to fetch inflation expectations: %s", e)

    if any(kw in q for kw in ['yield curve', 'inversion', 'inverted', '10y', '10-year']):
        try:
            _, r = run_query(
                "SELECT DATE, TREASURY_10Y, FED_FUNDS_RATE, CURVE_SPREAD, IS_INVERTED "
                "FROM V_YIELD_CURVE ORDER BY DATE DESC LIMIT 1"
            )
            if r:
                parts.append(
                    f"Yield curve as of {r[0][0]}: 10Y={r[0][1]*100:.2f}%, "
                    f"FFR={r[0][2]*100:.2f}%, spread={r[0][3]*100:.2f}%"
                    + (" (INVERTED)" if r[0][4] else "")
                )
        except Exception as e:
            logger.warning("Failed to fetch yield curve: %s", e)

    return '\n'.join(parts)


# ─────────────────────────────────────────────────────────────────────
# LLM helpers
# ─────────────────────────────────────────────────────────────────────
def ask_llm(question, context=''):
    cfg = LEVELS[st.session_state.level]

    history_lines = []
    recent = st.session_state.messages[-(MAX_HISTORY_TURNS * 2):]
    for msg in recent:
        role = "User" if msg['role'] == 'user' else "Assistant"
        history_lines.append(f"{role}: {msg['content'][:300]}")
    history = "\n".join(history_lines)

    augmented = question
    if context:
        augmented += f"\n\nRelevant market data:\n{context}"

    prompt = cfg['system_prompt']
    if history:
        prompt += f"\n\nRecent conversation:\n{history}"
    prompt += f"\n\nQuestion: {augmented}"

    return run_query_single(
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', %s)", (prompt,)
    )


def stream_text(text):
    for word in text.split(' '):
        yield word + ' '
        time.sleep(0.03)


# =====================================================================
#  PAGE 1 — LANDING  (orbiting planet cards, pure HTML, no st.button)
# =====================================================================
if st.session_state.level is None:

    hour = datetime.datetime.now().hour
    greeting = (
        'Good morning' if hour < 12
        else ('Good afternoon' if hour < 18 else 'Good evening')
    )
    _today_str = datetime.date.today().strftime('%b %d, %Y')

    _landing = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* ── Hide Streamlit chrome ───────────────────────────────────────── */
    [data-testid="collapsedControl"]           { display:none !important; }
    section[data-testid="stSidebar"]            { display:none !important; }
    header[data-testid="stHeader"]              { display:none !important; }
    .block-container,
    [data-testid="stAppViewBlockContainer"]     { padding:0 !important; margin:0 !important; max-width:100% !important; }
    /* Full dark page */
    [data-testid="stApp"], .stApp              { background:#07071a !important; }

    /* ── Reset ───────────────────────────────────────────────────────── */
    .ml-land * { box-sizing:border-box; margin:0; padding:0; }
    .ml-land   { font-family:'Inter',sans-serif; }

    /* ── Page wrapper ────────────────────────────────────────────────── */
    .ml-land {
        position:relative; min-height:100vh; width:100%;
        background:
            radial-gradient(ellipse 90% 55% at 50% -5%,  rgba(102,126,234,0.18) 0%, transparent 65%),
            radial-gradient(ellipse 55% 40% at 10% 110%, rgba(118,75,162,0.13) 0%, transparent 60%),
            #07071a;
        display:flex; flex-direction:column; align-items:center;
        overflow:hidden; padding-bottom:1.5rem;
    }
    /* Dot-grid texture */
    .ml-land::before {
        content:''; position:absolute; inset:0; pointer-events:none;
        background-image: radial-gradient(circle, rgba(102,126,234,0.10) 1px, transparent 1px);
        background-size: 38px 38px;
    }

    /* ── Top status bar ──────────────────────────────────────────────── */
    .ml-topbar {
        width:100%; display:flex; justify-content:space-between; align-items:center;
        padding:1.1rem 2.5rem; position:relative; z-index:10;
    }
    .ml-topbar-brand {
        font-size:0.82rem; font-weight:700; letter-spacing:0.12em;
        text-transform:uppercase; color:rgba(140,155,255,0.8);
    }
    .ml-topbar-right {
        display:flex; align-items:center; gap:0.55rem;
        font-size:0.75rem; color:rgba(140,150,200,0.65);
    }
    .ml-live-dot {
        width:7px; height:7px; border-radius:50%; background:#00e676; flex-shrink:0;
        animation:mlDotPulse 2s ease-in-out infinite;
    }
    @keyframes mlDotPulse {
        0%,100% { opacity:1;   box-shadow:0 0 0 0   rgba(0,230,118,0.5); }
        50%      { opacity:0.7; box-shadow:0 0 0 5px rgba(0,230,118,0);   }
    }

    /* ── Hero ────────────────────────────────────────────────────────── */
    .ml-hero {
        text-align:center; position:relative; z-index:5;
        margin-top:0.2rem; margin-bottom:1.4rem;
    }
    .ml-hero-icon { font-size:2.8rem; line-height:1; margin-bottom:0.3rem; }
    .ml-hero-title {
        font-size:3.6rem; font-weight:800; letter-spacing:-1.5px; line-height:1.05;
        background:linear-gradient(135deg, #b0bdff 0%, #7b8ff7 35%, #9f6edd 100%);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
    }
    .ml-hero-sub {
        font-size:0.98rem; font-weight:300; margin-top:0.45rem;
        color:rgba(170,178,230,0.75); letter-spacing:0.01em;
    }

    /* ── Tech pills ──────────────────────────────────────────────────── */
    .ml-pills {
        display:flex; justify-content:center; flex-wrap:wrap; gap:0.45rem;
        margin-top:1.1rem; position:relative; z-index:5;
    }
    .ml-pill {
        font-size:0.68rem; font-weight:600; letter-spacing:0.06em; text-transform:uppercase;
        padding:0.25rem 0.7rem; border-radius:999px;
        border:1px solid rgba(102,126,234,0.38);
        background:rgba(102,126,234,0.09);
        color:rgba(170,185,255,0.88);
    }

    /* ── Orbit scene ─────────────────────────────────────────────────── */
    .ml-orbit-scene {
        position:relative; width:100%; height:430px;
        display:flex; justify-content:center; align-items:center;
        overflow:visible; z-index:5;
    }
    .ml-orbit-trail {
        position:absolute; top:50%; left:50%;
        width:820px; height:420px;
        transform:translate(-50%,-50%);
        border:1px solid rgba(102,126,234,0.11);
        border-radius:50%; pointer-events:none;
    }
    .ml-orbit-trail::after {
        content:''; position:absolute; inset:-4px; border-radius:50%;
        border:1px dashed rgba(102,126,234,0.06);
    }

    /* Center card */
    .ml-center-card {
        position:relative; z-index:10; text-align:center; pointer-events:none;
        padding:1.4rem 2.8rem; border-radius:20px;
        background:rgba(12,12,32,0.88);
        border:1.5px solid rgba(102,126,234,0.28);
        backdrop-filter:blur(16px);
        box-shadow:0 0 50px rgba(102,126,234,0.14), 0 0 100px rgba(118,75,162,0.07),
                   inset 0 1px 0 rgba(255,255,255,0.04);
    }
    .ml-center-title {
        font-size:1.55rem; font-weight:700;
        background:linear-gradient(135deg,#a0b0ff,#7b8ff7);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text;
    }
    .ml-center-sub {
        font-size:0.82rem; color:rgba(140,150,200,0.72); margin-top:0.3rem; font-weight:300;
    }
    .ml-center-arrow {
        margin-top:0.6rem; font-size:0.75rem; color:rgba(102,126,234,0.55);
        letter-spacing:0.05em; text-transform:uppercase; font-weight:500;
    }

    /* Orbit mechanics */
    .ml-orbit-ellipse {
        position:absolute; top:50%; left:50%; width:0; height:0;
        transform:scaleX(1.72);
    }
    .ml-orbit-ring { animation:mlOrbitSpin 34s linear infinite; will-change:transform; }
    @keyframes mlOrbitSpin { from{transform:rotate(0deg);} to{transform:rotate(360deg);} }

    .ml-orbit-slot {
        position:absolute; width:200px;
        margin-left:-100px; margin-top:-90px; top:0; left:0;
    }
    .ml-slot-1 { transform:translateY(-232px); }
    .ml-slot-2 { transform:rotate(120deg) translateY(-232px); }
    .ml-slot-3 { transform:rotate(240deg) translateY(-232px); }

    a.ml-planet { display:block; text-decoration:none; color:white; cursor:pointer; }
    .ml-slot-1 a.ml-planet { animation:mlC1 34s linear infinite; }
    .ml-slot-2 a.ml-planet { animation:mlC2 34s linear infinite; }
    .ml-slot-3 a.ml-planet { animation:mlC3 34s linear infinite; }
    @keyframes mlC1 { from{transform:rotate(0deg)    scaleX(0.581)} to{transform:rotate(-360deg) scaleX(0.581)} }
    @keyframes mlC2 { from{transform:rotate(-120deg) scaleX(0.581)} to{transform:rotate(-480deg) scaleX(0.581)} }
    @keyframes mlC3 { from{transform:rotate(-240deg) scaleX(0.581)} to{transform:rotate(-600deg) scaleX(0.581)} }

    /* Planet cards */
    .ml-p-card {
        padding:1.2rem 1rem; border-radius:18px; text-align:center; color:white;
        border:1px solid rgba(255,255,255,0.13);
        box-shadow:0 8px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,0.08);
        transition:transform .22s, box-shadow .22s;
    }
    .ml-p-card:hover { transform:scale(1.09); box-shadow:0 18px 52px rgba(0,0,0,.55); }
    .ml-p-icon { font-size:1.9rem; margin-bottom:.35rem; }
    .ml-p-name { font-size:0.95rem; font-weight:700; margin-bottom:.2rem; }
    .ml-p-tag  { font-size:.70rem; opacity:.85; line-height:1.4; margin-bottom:.5rem; }
    .ml-p-divider { border:none; border-top:1px solid rgba(255,255,255,0.15); margin:.45rem 0; }
    .ml-p-feat { font-size:.62rem; opacity:.75; line-height:1.7; text-align:left; padding:0 .2rem; }

    .ml-grad-green  { background:linear-gradient(135deg, rgba(43,210,120,.90), rgba(30,200,175,.90)); }
    .ml-grad-purple { background:linear-gradient(135deg, rgba(102,126,234,.90), rgba(118,75,162,.90)); }
    .ml-grad-orange { background:linear-gradient(135deg, rgba(240,140,30,.90), rgba(255,200,0,.90)); }

    /* ── Bottom feature strip ────────────────────────────────────────── */
    .ml-strip {
        display:flex; justify-content:center; flex-wrap:wrap; gap:0.25rem 1.8rem;
        position:relative; z-index:5; margin-top:0.2rem; padding:0 1rem;
    }
    .ml-strip-item {
        display:flex; align-items:center; gap:0.35rem;
        font-size:0.73rem; color:rgba(140,150,200,0.60); font-weight:500;
        white-space:nowrap;
    }
    .ml-strip-dot { font-size:0.65rem; color:rgba(102,126,234,0.45); }
    </style>

    <div class="ml-land">

      <!-- Top status bar -->
      <div class="ml-topbar">
        <div class="ml-topbar-brand">MarketLens</div>
        <div class="ml-topbar-right">
          <div class="ml-live-dot"></div>
          Kafka streaming active &nbsp;·&nbsp; __DATE__
        </div>
      </div>

      <!-- Hero -->
      <div class="ml-hero">
        <div class="ml-hero-icon">🔍</div>
        <div class="ml-hero-title">MarketLens</div>
        <div class="ml-hero-sub">AI-powered market intelligence &nbsp;·&nbsp; real-time signals &nbsp;·&nbsp; multi-source data</div>
      </div>

      <!-- Tech pills -->
      <div class="ml-pills">
        <span class="ml-pill">⚡ Kafka</span>
        <span class="ml-pill">❄️ Snowflake</span>
        <span class="ml-pill">🔧 dbt</span>
        <span class="ml-pill">🏛 FRED API</span>
        <span class="ml-pill">🤖 Cortex AI</span>
        <span class="ml-pill">✈️ Airflow</span>
        <span class="ml-pill">📈 yfinance</span>
        <span class="ml-pill">🗂 SEC EDGAR</span>
      </div>

      <!-- Orbit scene -->
      <div class="ml-orbit-scene">
        <div class="ml-orbit-trail"></div>

        <div class="ml-center-card">
          <div class="ml-center-title">__GREETING__!</div>
          <div class="ml-center-sub">Choose your experience below to get started</div>
          <div class="ml-center-arrow">↻ click any card</div>
        </div>

        <div class="ml-orbit-ellipse">
          <div class="ml-orbit-ring">

            <div class="ml-orbit-slot ml-slot-1">
              <a href="?level=curious" class="ml-planet">
                <div class="ml-p-card ml-grad-green">
                  <div class="ml-p-icon">🌱</div>
                  <div class="ml-p-name">Just Curious</div>
                  <div class="ml-p-tag">Zero finance background —<br>explain like I'm five!</div>
                  <hr class="ml-p-divider">
                  <div class="ml-p-feat">
                    📖 Plain-English market story<br>
                    📊 Live price snapshot<br>
                    💬 AI chat with analogies
                  </div>
                </div>
              </a>
            </div>

            <div class="ml-orbit-slot ml-slot-2">
              <a href="?level=intermediate" class="ml-planet">
                <div class="ml-p-card ml-grad-purple">
                  <div class="ml-p-icon">📊</div>
                  <div class="ml-p-name">Know the Basics</div>
                  <div class="ml-p-tag">I understand stocks &amp; bonds —<br>show me what's moving.</div>
                  <hr class="ml-p-divider">
                  <div class="ml-p-feat">
                    🚀 Top gainers &amp; losers<br>
                    🔔 Live signal feed<br>
                    📰 AI market insights
                  </div>
                </div>
              </a>
            </div>

            <div class="ml-orbit-slot ml-slot-3">
              <a href="?level=analyst" class="ml-planet">
                <div class="ml-p-card ml-grad-orange">
                  <div class="ml-p-icon">🎯</div>
                  <div class="ml-p-name">Financial Analyst</div>
                  <div class="ml-p-tag">Z-scores, signals, macro —<br>give me everything.</div>
                  <hr class="ml-p-divider">
                  <div class="ml-p-feat">
                    📉 Signal heatmap &amp; deep dive<br>
                    🏦 7 macro overlays<br>
                    ⚡ Kafka live feed
                  </div>
                </div>
              </a>
            </div>

          </div>
        </div>
      </div>

      <!-- Bottom data strip -->
      <div class="ml-strip">
        <span class="ml-strip-item">📡 9 tickers monitored</span>
        <span class="ml-strip-dot">·</span>
        <span class="ml-strip-item">🔔 10 signal types</span>
        <span class="ml-strip-dot">·</span>
        <span class="ml-strip-item">🏦 7 macro indicators</span>
        <span class="ml-strip-dot">·</span>
        <span class="ml-strip-item">🔧 13 dbt models</span>
        <span class="ml-strip-dot">·</span>
        <span class="ml-strip-item">📄 SEC 10-K/10-Q filings</span>
        <span class="ml-strip-dot">·</span>
        <span class="ml-strip-item">⚡ Kafka 4-consumer fan-out</span>
      </div>

    </div>
    """.replace("__GREETING__", greeting).replace("__DATE__", _today_str)

    st.html(_landing)

    st.stop()


# =====================================================================
#  PAGE 2 — MAIN CHAT INTERFACE
# =====================================================================
cfg = LEVELS[st.session_state.level]
today = datetime.date.today()

weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
            'Saturday', 'Sunday']

with st.spinner("Loading..."):
    freshness = get_data_freshness()
freshness_str = f" · Data as of {freshness}" if freshness else ""

st.markdown(f"""
<div class="topbar">
    <div class="topbar-left">🔍 MarketLens &nbsp;·&nbsp; {cfg['icon']} {cfg['label']}</div>
    <div class="topbar-right">📅 {weekdays[today.weekday()]}, {today.strftime('%B %d, %Y')}{freshness_str}</div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    # Admin Mode toggle — always visible at top of sidebar
    _admin_col1, _admin_col2 = st.columns([1, 1])
    with _admin_col2:
        st.session_state.admin_mode = st.toggle(
            '⚙ Admin',
            value=st.session_state.admin_mode,
            help='Show tech-stack explainer notes on every page — great for demos & presentations.',
        )
    st.markdown(f"### {cfg['icon']} {cfg['label']} Mode")
    if st.button('↩ Change level', use_container_width=True):
        st.session_state.level = None
        st.session_state.messages = []
        st.session_state._transitioning = True
        st.rerun()

    if cfg['show_signals']:
        st.markdown('---')
        _page_options = ['Chat', 'Stock Deep Dive', 'Macro Overlay', 'Pipeline Health', 'Live Feed']
        _chosen_page = st.radio(
            'Navigate',
            _page_options,
            index=_page_options.index(
                st.session_state.page.replace('_', ' ').title()
                if st.session_state.page != 'chat' else 'Chat'
            ),
            key='_page_radio',
            label_visibility='collapsed',
        )
        _page_key = _chosen_page.lower().replace(' ', '_')
        if _page_key != st.session_state.page:
            st.session_state.page = _page_key
            st.rerun()

    if cfg['show_signals'] and st.session_state.page != 'chat':
        pass  # sidebar-only content for non-chat pages handled below

    if cfg['show_market_pulse']:
        st.markdown('---')
        st.markdown('**📈 Market Pulse**')
        with st.spinner("Loading market data..."):
            _pulse = {}
            for t in PULSE_TICKERS:
                try:
                    p, c, _ = get_latest_price(t)
                    _pulse[t] = (p, c) if p is not None else None
                except Exception:
                    _pulse[t] = None
        for t in PULSE_TICKERS:
            if _pulse.get(t) is not None:
                p, c = _pulse[t]
                arrow = '▲' if c >= 0 else '▼'
                color = '🟢' if c >= 0 else '🔴'
                st.markdown(f"{color} **{t}** ${p:.0f} {arrow}{abs(c):.1f}%")
            else:
                st.caption(f"⚠ {t}: unavailable")

    if cfg['show_signals']:
        st.markdown('---')
        st.markdown('**🔔 Top Signals**')
        with st.spinner("Loading signals..."):
            try:
                sdf = get_top_signals(4)
            except Exception:
                sdf = pd.DataFrame()
        if not sdf.empty:
            for idx, row in sdf.iterrows():
                magnitude = row['MAGNITUDE']  # may be None for non-price signals
                sig_type   = row.get('SIGNAL_TYPE', '')
                if magnitude is not None:
                    icon = '📈' if magnitude >= 0 else '📉'
                    header = f"{icon} **{row['ENTITY']}** {magnitude:+.1f}%"
                elif sig_type == 'SEC_FILING':
                    icon = '📄'
                    header = f"{icon} **{row['ENTITY']}**"
                else:
                    icon = '📊'
                    header = f"{icon} **{row['ENTITY']}**"
                summary_text = f"{header}  \n{row['SUMMARY']}"
                details = get_signal_details(
                    row['SIGNAL_TYPE'], row['ENTITY'], row['DATE'],
                )
                if details:
                    detail_str = ' · '.join(f"{k}: {v}" for k, v in details.items())
                    st.caption(f"{summary_text}  \n🔎 {detail_str}")
                else:
                    st.caption(summary_text)
        else:
            st.caption("⚠ Could not load signals.")

        # SEC filing tone breakdown
        st.markdown('---')
        st.markdown('**📄 SEC Filing Tones**')
        try:
            _tone_df = get_sec_tone_breakdown()
        except Exception:
            _tone_df = pd.DataFrame()
        if not _tone_df.empty:
            _tone_icons = {
                'POSITIVE': '🟢', 'NEUTRAL': '⚪', 'CAUTIOUS': '🟡', 'NEGATIVE': '🔴', 'UNKNOWN': '⬜',
            }
            for _, _tr in _tone_df.iterrows():
                _tico = _tone_icons.get(_tr['TONE'], '⬜')
                st.caption(f"{_tico} {_tr['TONE'].title()}: **{int(_tr['CNT'])}** filings")
        else:
            st.caption("No filing data available.")

    if cfg['show_market_pulse']:
        st.markdown('---')
        st.markdown('**📊 Price Chart**')
        sb_ticker = st.selectbox(
            'Ticker', TICKERS,
            format_func=lambda t: f'{TICKER_NAMES.get(t, t)} ({t})',
            key='sb_ticker', label_visibility='collapsed',
        )
        sb_period = st.radio(
            'Period', ['30D', '3M', '6M', '1Y'],
            horizontal=True, key='sb_period', label_visibility='collapsed',
        )
        sb_days = {'30D': 30, '3M': 90, '6M': 180, '1Y': 365}[sb_period]
        with st.spinner("Loading chart..."):
            try:
                sb_df = get_price_data(sb_ticker, sb_days)
            except Exception:
                sb_df = pd.DataFrame()
        if not sb_df.empty:
            first_p = sb_df.iloc[0]['CLOSE_PRICE']
            last_p = sb_df.iloc[-1]['CLOSE_PRICE']
            ret = (last_p - first_p) / first_p * 100
            st.metric(
                TICKER_NAMES.get(sb_ticker, sb_ticker),
                f'${last_p:.2f}', f'{ret:+.1f}%',
            )
            st.area_chart(
                sb_df.set_index('DATE')['CLOSE_PRICE'],
                color='#00c853' if ret >= 0 else '#ff1744',
            )
        else:
            st.caption("⚠ Could not load price chart.")

# =====================================================================
#  PAGE: STOCK DEEP DIVE
# =====================================================================
if st.session_state.page == 'stock_deep_dive':
    st.markdown('#### 📈 Stock Deep Dive')
    _tech_note(
        'Stock Deep Dive — yfinance → Snowflake → dbt → Z-score',
        'Price ingestion: `ingestion/stock_producer.py` fetches OHLCV from yfinance → '
        'writes to `RAW_STOCK_PRICES` in Snowflake. '
        'dbt staging model `stg_stock_prices` normalises the data. '
        'dbt mart model `anomaly_scores` computes a 20-day rolling Z-score for each ticker\'s daily return → '
        'exposed as the Snowflake view `V_ANOMALY_SCORES` queried by this page.',
    )
    _render_stats_ribbon()

    dd_ticker = st.selectbox(
        'Select Ticker',
        TICKERS,
        format_func=lambda t: f'{TICKER_NAMES.get(t, t)} ({t})',
        key='dd_ticker',
    )
    dd_period = st.radio('Period', ['30D', '90D', '180D'], horizontal=True, key='dd_period')
    dd_days   = {'30D': 30, '90D': 90, '180D': 180}[dd_period]

    # ── Ticker summary header cards ────────────────────────────────────
    with st.spinner('Loading indicator summary...'):
        _tsig = get_ticker_signal_summary(dd_ticker)
    _hc = st.columns(4)
    # RSI card
    with _hc[0]:
        _rsi_val  = _tsig.get('rsi')
        _rsi_st   = _tsig.get('rsi_state', '—')
        _rsi_col  = ('#ff4444' if _rsi_st == 'OVERBOUGHT'
                     else '#44aaff' if _rsi_st == 'OVERSOLD' else '#aaaaaa')
        st.markdown(
            f"""<div style="background:rgba(20,20,40,0.7);border-radius:10px;
                border:1px solid rgba(102,126,234,0.3);padding:0.7rem;text-align:center;">
                <div style="font-size:0.72rem;color:#999;">RSI (14)</div>
                <div style="font-size:1.5rem;font-weight:700;color:{_rsi_col};">
                    {f'{_rsi_val:.1f}' if _rsi_val is not None else '—'}
                </div>
                <div style="font-size:0.7rem;color:{_rsi_col};">{_rsi_st}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    # MA crossover card
    with _hc[1]:
        _ma_ev  = _tsig.get('ma_event', '—') or '—'
        _ma_col = ('#44cc44' if _ma_ev == 'GOLDEN_CROSS'
                   else '#ff4444' if _ma_ev == 'DEATH_CROSS' else '#aaaaaa')
        _ma_icon = '☀️' if _ma_ev == 'GOLDEN_CROSS' else '💀' if _ma_ev == 'DEATH_CROSS' else '—'
        st.markdown(
            f"""<div style="background:rgba(20,20,40,0.7);border-radius:10px;
                border:1px solid rgba(102,126,234,0.3);padding:0.7rem;text-align:center;">
                <div style="font-size:0.72rem;color:#999;">MA Crossover</div>
                <div style="font-size:1.5rem;">{_ma_icon}</div>
                <div style="font-size:0.7rem;color:{_ma_col};">{_ma_ev.replace('_',' ')}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    # Drawdown card
    with _hc[2]:
        _dd_pct = _tsig.get('drawdown_pct')
        _dd_st  = _tsig.get('drawdown_state', '—') or '—'
        _dd_col = ('#ff4444' if _dd_st in ('BEAR_MARKET', 'CORRECTION')
                   else '#ffaa44' if _dd_st == 'PULLBACK' else '#aaaaaa')
        st.markdown(
            f"""<div style="background:rgba(20,20,40,0.7);border-radius:10px;
                border:1px solid rgba(102,126,234,0.3);padding:0.7rem;text-align:center;">
                <div style="font-size:0.72rem;color:#999;">Drawdown from 52W High</div>
                <div style="font-size:1.5rem;font-weight:700;color:{_dd_col};">
                    {f'{_dd_pct*100:+.1f}%' if _dd_pct is not None else '—'}
                </div>
                <div style="font-size:0.7rem;color:{_dd_col};">{_dd_st.replace('_',' ')}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    # Anomaly count card
    with _hc[3]:
        with st.spinner(''):
            try:
                _adf = get_anomaly_data(dd_ticker, dd_days)
                _anom_cnt = int((_adf['IS_ANOMALY'] == True).sum()) if not _adf.empty else 0
            except Exception:
                _anom_cnt = 0
        _anom_col = '#ff4444' if _anom_cnt > 3 else '#ffaa44' if _anom_cnt > 0 else '#aaaaaa'
        st.markdown(
            f"""<div style="background:rgba(20,20,40,0.7);border-radius:10px;
                border:1px solid rgba(102,126,234,0.3);padding:0.7rem;text-align:center;">
                <div style="font-size:0.72rem;color:#999;">Anomalies in Period</div>
                <div style="font-size:1.5rem;font-weight:700;color:{_anom_col};">{_anom_cnt}</div>
                <div style="font-size:0.7rem;color:{_anom_col};">
                    {'high activity' if _anom_cnt > 3 else 'some activity' if _anom_cnt > 0 else 'quiet'}
                </div>
            </div>""",
            unsafe_allow_html=True,
        )
    st.markdown('<div style="margin-bottom:0.8rem;"></div>', unsafe_allow_html=True)

    with st.spinner('Loading anomaly data...'):
        try:
            dd_df = get_anomaly_data(dd_ticker, dd_days)
        except Exception as _e:
            dd_df = pd.DataFrame()
            st.warning(f'Could not load anomaly data: {_e}')

    if not dd_df.empty:
        dd_df['DATE'] = pd.to_datetime(dd_df['DATE'])
        dd_df = dd_df.sort_values('DATE')
        dd_df['DAILY_RETURN_PCT'] = dd_df['DAILY_RETURN'] * 100
        dd_df['VOLATILITY_20D_PCT'] = dd_df['VOLATILITY_20D'] * 100

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('**Daily Return & Z-Score**')
            st.line_chart(dd_df.set_index('DATE')[['DAILY_RETURN_PCT', 'Z_SCORE']])
        with col2:
            st.markdown('**20-Day Rolling Volatility (%)**')
            st.line_chart(dd_df.set_index('DATE')[['VOLATILITY_20D_PCT']])

        anomalies = dd_df[dd_df['IS_ANOMALY'] == True].copy()
        if not anomalies.empty:
            anomalies['DAILY_RETURN_PCT'] = anomalies['DAILY_RETURN_PCT'].map('{:+.2f}%'.format)
            anomalies['Z_SCORE'] = anomalies['Z_SCORE'].map('{:.2f}'.format)
            st.markdown(f'**Anomaly Days ({len(anomalies)} found)**')
            st.dataframe(
                anomalies[['DATE', 'DAILY_RETURN_PCT', 'Z_SCORE']].rename(columns={
                    'DATE': 'Date', 'DAILY_RETURN_PCT': 'Return', 'Z_SCORE': 'Z-Score',
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info('No anomaly days in the selected period.')
    else:
        st.info('No data available for this ticker.')

    # ── Signal Heatmap ─────────────────────────────────────────────────
    st.markdown('---')
    st.markdown('#### 🔥 Signal Heatmap — All Tickers')
    st.caption('Counts of each signal type fired per ticker across all recorded history.')
    with st.spinner('Loading heatmap...'):
        try:
            _hm_df = get_signal_heatmap_data()
        except Exception:
            _hm_df = pd.DataFrame()
    if not _hm_df.empty:
        _pivot = (
            _hm_df.pivot_table(index='ENTITY', columns='SIGNAL_TYPE', values='CNT', aggfunc='sum', fill_value=0)
            .reindex(index=[t for t in WATCHLIST_TICKERS if t in _hm_df['ENTITY'].values])
        )
        if not _pivot.empty:
            # Style: background gradient per column so each signal type scales independently
            _styled = (
                _pivot.style
                .background_gradient(cmap='YlOrRd', axis=0)
                .format('{:.0f}')
            )
            st.dataframe(_styled, use_container_width=True)
        else:
            st.info('Not enough data to build heatmap yet.')
    else:
        st.info('Heatmap data unavailable.')

    st.stop()

# =====================================================================
#  PAGE: MACRO OVERLAY
# =====================================================================
if st.session_state.page == 'macro_overlay':
    st.markdown('#### 🏦 Macro Overlay')
    _tech_note(
        'Macro Overlay — Snowflake Marketplace + FRED API + dbt',
        'Fed Funds Rate & CPI: free Snowflake Marketplace dataset (`SNOWFLAKE_PUBLIC_DATA_FREE`) '
        '→ dbt staging models → `V_FED_RATE_CHANGES`, `V_CPI_CHANGES`. '
        'Yield Curve, GDP, Housing Starts, Consumer Sentiment, Inflation Expectations: '
        'fetched via FRED API (St. Louis Fed, free key) by `ingestion/fred_producer.py` '
        '→ `RAW_FRED_INDICATORS` (Snowflake) '
        '→ dbt staging (`stg_fred_*`) → mart models '
        '→ `V_YIELD_CURVE`, `V_GDP_CHANGES`, `V_FRED_HOUSING`, `V_SENTIMENT_CHANGES`, `V_FRED_INFLATION_EXPECTATIONS`.',
    )
    _render_stats_ribbon()

    from config import FRED_API_KEY
    _fred_available = bool(FRED_API_KEY)

    with st.spinner('Loading macro data...'):
        try:
            fed_df = get_fed_rate_data()
        except Exception as _e:
            fed_df = pd.DataFrame()
            st.warning(f'Fed rate data unavailable: {_e}')
        try:
            cpi_df = get_cpi_data()
        except Exception as _e:
            cpi_df = pd.DataFrame()
            st.warning(f'CPI data unavailable: {_e}')
        yc_df       = get_yield_curve_data()        if _fred_available else pd.DataFrame()
        gdp_df      = get_gdp_data()                if _fred_available else pd.DataFrame()
        housing_df  = get_housing_data()             if _fred_available else pd.DataFrame()
        sent_df     = get_sentiment_data()           if _fred_available else pd.DataFrame()
        infexp_df   = get_inflation_expectations_data() if _fred_available else pd.DataFrame()

    _fred_note = (
        'Requires a free FRED API key — set `FRED_API_KEY` in `.env` then run '
        '`python run_fred_ingest.py`.'
    )

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        'Fed Funds Rate', 'CPI', 'Yield Curve',
        'GDP', 'Housing Starts', 'Consumer Sentiment', 'Inflation Expectations',
    ])

    with tab1:
        if not fed_df.empty:
            fed_df['DATE'] = pd.to_datetime(fed_df['DATE'])
            fed_df = fed_df.sort_values('DATE')
            fed_df['FED_FUNDS_RATE_PCT'] = fed_df['FED_FUNDS_RATE'] * 100
            st.line_chart(fed_df.set_index('DATE')[['FED_FUNDS_RATE_PCT']])
            changes = fed_df[fed_df['RATE_CHANGE'].notna() & (fed_df['RATE_CHANGE'] != 0)].copy()
            if not changes.empty:
                changes['RATE_CHANGE_BPS'] = (changes['RATE_CHANGE'] * 10000).map('{:+.1f} bps'.format)
                st.markdown('**Rate Changes**')
                st.dataframe(changes[['DATE', 'FED_FUNDS_RATE_PCT', 'RATE_CHANGE_BPS']].rename(columns={
                    'DATE': 'Date', 'FED_FUNDS_RATE_PCT': 'Rate (%)', 'RATE_CHANGE_BPS': 'Change',
                }), use_container_width=True, hide_index=True)
        else:
            st.info('No Fed rate data available.')

    with tab2:
        if not cpi_df.empty:
            cpi_df['DATE'] = pd.to_datetime(cpi_df['DATE'])
            cpi_df = cpi_df.sort_values('DATE')
            cpi_df['MOM_PCT'] = cpi_df['CPI_MOM_CHANGE'] * 100
            col1, col2 = st.columns(2)
            with col1:
                st.markdown('**CPI Index Level**')
                st.line_chart(cpi_df.set_index('DATE')[['CPI_INDEX']])
            with col2:
                st.markdown('**Month-over-Month Change (%)**')
                st.bar_chart(cpi_df.set_index('DATE')[['MOM_PCT']])
        else:
            st.info('No CPI data available.')

    with tab3:
        if not yc_df.empty:
            yc_df['DATE'] = pd.to_datetime(yc_df['DATE'])
            yc_df = yc_df.sort_values('DATE')
            inversion_count = int(yc_df['IS_INVERTED'].sum()) if 'IS_INVERTED' in yc_df.columns else 0
            if inversion_count > 0:
                st.error(f'Yield curve currently inverted in {inversion_count} of {len(yc_df)} periods')
            else:
                st.success('Yield curve is not inverted in this window')
            if 'TREASURY_10Y' in yc_df.columns:
                st.line_chart(yc_df.set_index('DATE')[['TREASURY_10Y', 'FED_FUNDS_RATE', 'CURVE_SPREAD']])
        else:
            st.info('No yield curve data available.' if _fred_available else _fred_note)

    with tab4:
        if not gdp_df.empty:
            gdp_df['DATE'] = pd.to_datetime(gdp_df['DATE'])
            gdp_df = gdp_df.sort_values('DATE')
            contractions = int(gdp_df['IS_CONTRACTION'].sum()) if 'IS_CONTRACTION' in gdp_df.columns else 0
            if contractions >= 2:
                st.error(f'{contractions} consecutive contraction quarters — possible recession signal')
            col1, col2 = st.columns(2)
            with col1:
                st.markdown('**Real GDP (Billions, SAAR)**')
                st.line_chart(gdp_df.set_index('DATE')[['GDP_REAL_BILLIONS']])
            with col2:
                st.markdown('**Quarter-over-Quarter Growth (%)**')
                st.bar_chart(gdp_df.set_index('DATE')[['QOQ_GROWTH_PCT']])
        else:
            st.info('No GDP data available.' if _fred_available else _fred_note)

    with tab5:
        if not housing_df.empty:
            housing_df['DATE'] = pd.to_datetime(housing_df['DATE'])
            housing_df = housing_df.sort_values('DATE')
            st.markdown('**Housing Starts (Thousands of Units, SAAR)**')
            st.line_chart(housing_df.set_index('DATE')[['HOUSING_STARTS_K']])
        else:
            st.info('No housing starts data available.' if _fred_available else _fred_note)

    with tab6:
        if not sent_df.empty:
            sent_df['DATE'] = pd.to_datetime(sent_df['DATE'])
            sent_df = sent_df.sort_values('DATE')
            col1, col2 = st.columns(2)
            with col1:
                st.markdown('**Consumer Sentiment Index (UMich)**')
                st.line_chart(sent_df.set_index('DATE')[['SENTIMENT_INDEX']])
            with col2:
                st.markdown('**Month-over-Month Change (pts)**')
                st.bar_chart(sent_df.set_index('DATE')[['MOM_CHANGE']])
            sharp = sent_df[sent_df['SENTIMENT_EVENT'].notna()].copy()
            if not sharp.empty:
                st.markdown('**Sharp Moves**')
                st.dataframe(sharp[['DATE', 'SENTIMENT_INDEX', 'MOM_CHANGE', 'SENTIMENT_EVENT']].rename(
                    columns={'DATE': 'Date', 'SENTIMENT_INDEX': 'Index',
                             'MOM_CHANGE': 'Change (pts)', 'SENTIMENT_EVENT': 'Event'}
                ), use_container_width=True, hide_index=True)
        else:
            st.info('No consumer sentiment data available.' if _fred_available else _fred_note)

    with tab7:
        if not infexp_df.empty:
            infexp_df['DATE'] = pd.to_datetime(infexp_df['DATE'])
            infexp_df = infexp_df.sort_values('DATE')
            infexp_df['INFEXP_PCT'] = infexp_df['INFLATION_EXPECTATION_PCT'] * 100
            st.markdown('**10Y Breakeven Inflation Rate (%)**')
            st.line_chart(infexp_df.set_index('DATE')[['INFEXP_PCT']])
            latest = infexp_df.iloc[-1]
            st.caption(
                f"Latest: {latest['INFEXP_PCT']:.2f}% as of {str(latest['DATE'])[:10]} — "
                "difference between 10Y nominal Treasury and 10Y TIPS yield (market-implied long-run inflation)"
            )
        else:
            st.info('No inflation expectations data available.' if _fred_available else _fred_note)
    st.stop()

# =====================================================================
#  PAGE: PIPELINE HEALTH
# =====================================================================
if st.session_state.page == 'pipeline_health':
    st.markdown('#### ⚙️ Pipeline Health')
    _tech_note(
        'Pipeline Health — Apache Airflow + Snowflake + Kafka Admin API',
        'Task run log: every ingestion task (Airflow DAG `marketlens_daily` and `run_fred_ingest.py`) '
        'calls `ingestion/pipeline_logger.py` which MERGEs a row into the Snowflake table '
        '`PIPELINE_RUN_LOG` — tracking task name, status, row count, and duration. '
        'Data Freshness: live row counts directly from `RAW_STOCK_PRICES` and `RAW_MACRO_INDICATORS`. '
        'Kafka Stream Health: `KafkaAdminClient` queries each consumer group\'s committed offsets vs. '
        'log-end offsets to compute per-group message lag in real time.',
    )
    _render_stats_ribbon()

    with st.spinner('Loading pipeline runs...'):
        runs_df, runs_err = get_pipeline_runs()

    if runs_err:
        st.warning(
            f'Pipeline log unavailable ({runs_err}). '
            'Run migrations/01_add_raw_tables.sql to create PIPELINE_RUN_LOG.'
        )
    elif runs_df.empty:
        st.info('No pipeline runs recorded yet. Trigger the marketlens_daily DAG to populate this table.')
    else:
        # Summary metrics
        total     = len(runs_df)
        completed = int((runs_df['STATUS'] == 'completed').sum())
        failed    = int((runs_df['STATUS'] == 'failed').sum())
        m1, m2, m3 = st.columns(3)
        m1.metric('Total Task Runs', total)
        m2.metric('Completed', completed)
        m3.metric('Failed', failed, delta=f'-{failed}' if failed else None,
                  delta_color='inverse')

        # Status breakdown bar
        if 'STATUS' in runs_df.columns:
            status_counts = runs_df['STATUS'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            st.bar_chart(status_counts.set_index('Status'))

        # Raw log table
        st.markdown('**Recent Task Runs**')
        display_cols = [c for c in ['TASK_ID', 'STATUS', 'ROW_COUNT',
                                     'DURATION_SEC', 'STARTED_AT', 'ERROR_MSG']
                        if c in runs_df.columns]
        st.dataframe(
            runs_df[display_cols].rename(columns={
                'TASK_ID': 'Task', 'STATUS': 'Status',
                'ROW_COUNT': 'Rows', 'DURATION_SEC': 'Sec',
                'STARTED_AT': 'Started', 'ERROR_MSG': 'Error',
            }),
            use_container_width=True,
            hide_index=True,
        )

    # Raw price row count as a quick data freshness check
    st.markdown('---')
    st.markdown('**Data Freshness**')
    col1, col2 = st.columns(2)
    with col1:
        try:
            _, r = run_query('SELECT MAX(DATE), COUNT(*) FROM SCORPION_DB.MARKETLENS.RAW_STOCK_PRICES')
            if r and r[0][0]:
                st.metric('Latest price date', str(r[0][0])[:10])
                st.metric('Total price rows', f'{r[0][1]:,}')
            else:
                st.info('RAW_STOCK_PRICES is empty.')
        except Exception as _e:
            st.caption(f'RAW_STOCK_PRICES unavailable: {_e}')
    with col2:
        try:
            _, r = run_query('SELECT MAX(DATE), COUNT(*) FROM SCORPION_DB.MARKETLENS.RAW_MACRO_INDICATORS')
            if r and r[0][0]:
                st.metric('Latest macro date', str(r[0][0])[:10])
                st.metric('Total macro rows', f'{r[0][1]:,}')
            else:
                st.info('RAW_MACRO_INDICATORS is empty.')
        except Exception as _e:
            st.caption(f'RAW_MACRO_INDICATORS unavailable: {_e}')

    # ── Kafka consumer lag ────────────────────────────────────────────
    st.markdown('---')
    st.markdown('**⚡ Kafka Stream Health**')
    lag = get_kafka_consumer_lag()
    if not lag:
        st.caption('Kafka not reachable — start with `docker compose -f docker-compose.kafka.yml up -d`')
    else:
        _lag_cols = st.columns(len(lag))
        _icons = {'sf-writer': '🗄️', 'anomaly-check': '🔍', 'notifier': '🔔', 'dashboard': '📺'}
        for i, (group, value) in enumerate(lag.items()):
            with _lag_cols[i]:
                if value is None:
                    st.metric(_icons.get(group, '') + ' ' + group, 'offline')
                else:
                    st.metric(_icons.get(group, '') + ' ' + group,
                              f'{value} msg lag',
                              delta='live' if value == 0 else f'{value} behind',
                              delta_color='normal' if value == 0 else 'inverse')
    st.stop()

# =====================================================================
#  PAGE: LIVE FEED
# =====================================================================
if st.session_state.page == 'live_feed':
    import json as _json

    _LIVE_FEED_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'live_feed.json',
    )

    st.markdown('#### ⚡ Live Price Feed')
    st.caption(
        'Simulated price ticks generated via GBM random walk, published by '
        '`tick_producer` → Kafka `raw.stock.prices` → `dashboard_consumer` → this page. '
        'Auto-refreshes every 2 seconds.'
    )
    _tech_note(
        'Live Feed — Kafka Multi-Consumer Fan-out Architecture',
        '`ingestion/tick_producer.py` generates one GBM price tick per ticker every 2 s '
        '→ publishes to Kafka topic `raw.stock.prices` (broker: localhost:9092). '
        'Four independent consumer groups each read this same stream with their own committed offsets: '
        '`sf-writer` (→ Snowflake `RAW_STOCK_PRICES`), '
        '`anomaly-check` (`ingestion/anomaly_consumer.py` — Z-score detector → publishes alerts to `signals.anomalies`), '
        '`notifier` (`ingestion/notifier_consumer.py` → Slack webhook / email), '
        '`dashboard` (`ingestion/dashboard_consumer.py` → writes `live_feed.json` atomically). '
        'This page reads `live_feed.json` and re-renders via `@st.fragment(run_every=2)` — '
        'no polling loop, Streamlit handles the re-execution automatically.',
    )
    _render_stats_ribbon()

    _status_placeholder = st.empty()
    _grid_placeholder   = st.empty()
    _anomaly_placeholder = st.empty()

    @st.fragment(run_every=2)
    def _live_feed_fragment():
        try:
            with open(_LIVE_FEED_PATH) as _f:
                _data = _json.load(_f)

            _updated = _data.get('updated_at', '')[:19].replace('T', ' ')
            _total   = _data.get('total_msgs', 0)
            _tickers = _data.get('tickers', {})

            _status_placeholder.caption(
                f'Last update: **{_updated} UTC** — '
                f'{_total:,} messages processed by dashboard consumer'
            )

            # Price grid — 3 columns
            _cols = _grid_placeholder.columns(3)
            for _i, _ticker in enumerate(sorted(_tickers)):
                _info = _tickers[_ticker]
                _price = _info.get('price', 0.0)
                _chg   = _info.get('change_pct', 0.0)
                with _cols[_i % 3]:
                    st.metric(
                        label=f"**{_ticker}**",
                        value=f"${_price:.2f}",
                        delta=f"{_chg:+.3f}%",
                    )

            # Anomaly feed
            _anomaly_placeholder.markdown('---')
            with _anomaly_placeholder.container():
                st.markdown('**Anomaly events** (detected by `anomaly_consumer`)')

                _ANOMALY_LOG = _LIVE_FEED_PATH.replace('live_feed.json', 'logs/anomaly_consumer.log')
                try:
                    with open(_ANOMALY_LOG) as _af:
                        _lines = _af.readlines()
                    _anomaly_lines = [_l.strip() for _l in _lines if 'ANOMALY' in _l][-10:]
                    if _anomaly_lines:
                        for _line in reversed(_anomaly_lines):
                            st.code(_line, language=None)
                    else:
                        st.caption('No anomalies detected yet in this session.')
                except FileNotFoundError:
                    st.caption('Anomaly log not found — streaming services may not be running.')

        except FileNotFoundError:
            _status_placeholder.info(
                'Live feed not active. Start streaming services with:\n\n'
                '```bash\n'
                'docker compose -f docker-compose.kafka.yml up -d\n'
                'bash start_streaming.sh\n'
                '```'
            )
        except Exception as _e:
            _status_placeholder.warning(f'Live feed error: {_e}')

    _live_feed_fragment()
    st.stop()

# ── What's Moving (Know the Basics + Analyst — only on fresh chat page) ──
if cfg['show_market_pulse'] and not st.session_state.messages:
    st.markdown('#### 🚀 What\'s Moving Today')
    with st.spinner('Loading movers...'):
        _movers: dict = {}
        for _t in WATCHLIST_TICKERS:
            try:
                _p, _c, _ = get_latest_price(_t)
                if _p is not None:
                    _movers[_t] = (_p, _c)
            except Exception:
                pass
    if _movers:
        _sorted_movers = sorted(_movers.items(), key=lambda x: x[1][1], reverse=True)
        _gainers = _sorted_movers[:3]
        _losers  = _sorted_movers[-3:]
        _mc1, _mc2 = st.columns(2)
        with _mc1:
            st.markdown('**📈 Top Gainers**')
            for _t, (_p, _c) in _gainers:
                _name = TICKER_NAMES.get(_t, _t)
                st.markdown(
                    f"""<div style="display:flex;justify-content:space-between;align-items:center;
                        padding:0.4rem 0.8rem;margin-bottom:0.3rem;border-radius:8px;
                        background:rgba(0,200,83,0.10);border:1px solid rgba(0,200,83,0.25);">
                        <span style="font-weight:600;color:#000;">{_name} <span style="color:#000;font-size:0.8rem;">({_t})</span></span>
                        <span style="font-weight:700;color:#00c853;">${_p:.2f} &nbsp; +{_c:.2f}%</span>
                    </div>""",
                    unsafe_allow_html=True,
                )
        with _mc2:
            st.markdown('**📉 Top Losers**')
            for _t, (_p, _c) in reversed(_losers):
                _name = TICKER_NAMES.get(_t, _t)
                st.markdown(
                    f"""<div style="display:flex;justify-content:space-between;align-items:center;
                        padding:0.4rem 0.8rem;margin-bottom:0.3rem;border-radius:8px;
                        background:rgba(255,23,68,0.10);border:1px solid rgba(255,23,68,0.25);">
                        <span style="font-weight:600;color:#000;">{_name} <span style="color:#000;font-size:0.8rem;">({_t})</span></span>
                        <span style="font-weight:700;color:#ff1744;">${_p:.2f} &nbsp; {_c:+.2f}%</span>
                    </div>""",
                    unsafe_allow_html=True,
                )
        # Signals count badge
        try:
            _sc = get_signal_stats()
            _sig_total = _sc.get('total', 0)
        except Exception:
            _sig_total = 0
        if _sig_total:
            st.markdown(
                f'<div style="margin-top:0.6rem;font-size:0.82rem;color:#444;">'
                f'🔔 <b style="color:#667eea;">{_sig_total:,} signals</b> detected across '
                f'<b style="color:#667eea;">{len(WATCHLIST_TICKERS)} tickers</b> and '
                f'<b style="color:#667eea;">10 signal types</b> — '
                f'see <i>Stock Deep Dive</i> for the full breakdown.</div>',
                unsafe_allow_html=True,
            )
    st.markdown('---')

# ── Top section: suggestions (left) + market insights (right) ────────
if cfg['show_signals']:
    _top_left, _top_right = st.columns([2.5, 1.2])
elif st.session_state.level == 'curious' and not st.session_state.messages:
    # Just Curious: give equal space for market snapshot on the right
    _top_left, _top_right = st.columns([1.4, 1])
else:
    _top_left, _top_right = st.columns([1, 0.001])

with _top_right:
    if cfg['show_signals']:
        st.markdown("#### 📰 Market Insights")
        _tech_note(
            'Market Insights — Snowflake Cortex LLM',
            'Top 6 signals are fetched from the Snowflake view `V_SIGNAL_SUMMARY` (built by dbt), '
            'formatted as context, and passed to `SNOWFLAKE.CORTEX.COMPLETE(\'llama3.1-70b\')` '
            'to generate news-style bullet points. Result is cached for 15 minutes.',
        )
        with st.spinner("Generating insights..."):
            try:
                insights = get_market_insights()
            except Exception:
                insights = None
        if insights:
            st.markdown(
                f'<div class="insight-card">{_format_insights_for_display(insights)}</div>',
                unsafe_allow_html=True,
            )
        else:
            st.caption("Insights unavailable at the moment.")
    elif st.session_state.level == 'curious' and not st.session_state.messages:
        # Just Curious: market snapshot panel
        st.markdown('#### 📊 Today\'s Market')
        _snapshot_tickers = ['SPY', 'QQQ', 'AAPL']
        _snap_names = {'SPY': 'S&P 500', 'QQQ': 'Nasdaq 100', 'AAPL': 'Apple'}
        with st.spinner(''):
            for _st_ticker in _snapshot_tickers:
                try:
                    _sp, _sc2, _ = get_latest_price(_st_ticker)
                    if _sp is not None:
                        _sup = _sc2 >= 0
                        _sbg = 'rgba(0,200,83,0.12)' if _sup else 'rgba(255,23,68,0.12)'
                        _sbc = 'rgba(0,200,83,0.3)' if _sup else 'rgba(255,23,68,0.3)'
                        _scolor = '#00c853' if _sup else '#ff1744'
                        _sarrow = '▲' if _sup else '▼'
                        st.markdown(
                            f"""<div style="background:{_sbg};border:1px solid {_sbc};
                                border-radius:12px;padding:0.9rem 1.1rem;margin-bottom:0.5rem;
                                display:flex;justify-content:space-between;align-items:center;">
                                <div>
                                    <div style="font-weight:700;font-size:1rem;color:#000;">{_snap_names[_st_ticker]}</div>
                                    <div style="font-size:0.75rem;color:#333;">{_st_ticker}</div>
                                </div>
                                <div style="text-align:right;">
                                    <div style="font-size:1.3rem;font-weight:700;color:#000;">${_sp:.2f}</div>
                                    <div style="font-size:0.9rem;font-weight:600;color:{_scolor};">{_sarrow} {abs(_sc2):.2f}%</div>
                                </div>
                            </div>""",
                            unsafe_allow_html=True,
                        )
                except Exception:
                    pass

with _top_left:
    if not st.session_state.messages:
        st.markdown('')
        st.markdown(f"##### {cfg['icon']} {cfg['tagline']}")
        # Just Curious: Story of the Day above suggestions
        if st.session_state.level == 'curious':
            with st.spinner('Generating today\'s market story...'):
                try:
                    _story = get_story_of_day()
                except Exception:
                    _story = None
            if _story:
                st.markdown(
                    f'<div style="background:linear-gradient(135deg,rgba(102,126,234,0.08),'
                    f'rgba(118,75,162,0.08));border-radius:12px;border:1px solid '
                    f'rgba(102,126,234,0.2);padding:0.9rem 1.1rem;margin-bottom:1rem;'
                    f'font-size:0.9rem;line-height:1.65;color:#222;">'
                    f'📖 <b style="color:#a8b4f8;">Today\'s Market Story</b><br>{html.escape(_story)}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        st.markdown("Try one of these, or type your own question:")
        cols = st.columns(len(cfg['suggestions']))
        for i, (btn_label, full_q) in enumerate(cfg['suggestions']):
            with cols[i]:
                if st.button(btn_label, use_container_width=True, key=f'sug_{i}'):
                    st.session_state.pending_q = full_q

# ── Chat history ─────────────────────────────────────────────────────
_tech_note(
    'Chat — RAG-style Context Augmentation + Snowflake Cortex',
    'When you ask a question, `build_context()` runs targeted SQL queries against Snowflake views '
    '(`V_STOCK_PRICES`, `V_SIGNAL_SUMMARY`, `V_FED_FUNDS_RATE`, `V_CPI_CHANGES`, '
    '`V_GDP_CHANGES`, `V_YIELD_CURVE`, `V_FRED_HOUSING`, `V_SENTIMENT_CHANGES`, etc.) '
    'based on keywords in your question. '
    'The retrieved data is injected into the prompt alongside the last 3 conversation turns, '
    'then sent to `SNOWFLAKE.CORTEX.COMPLETE(\'llama3.1-70b\')`. '
    'SEC filing narratives (10-K/10-Q summaries from `V_SEC_NARRATIVES`) are also injected when a ticker is mentioned.',
)
for msg in st.session_state.messages:
    with st.chat_message(msg['role'], avatar='🧑‍🎓' if msg['role'] == 'user' else '🔍'):
        st.markdown(msg['content'])

# ── Chat input ───────────────────────────────────────────────────────
user_input = st.chat_input('Ask me anything about the markets...')
if user_input:
    st.session_state.pending_q = user_input

# ── Process pending question ─────────────────────────────────────────
if st.session_state.pending_q:
    question = st.session_state.pending_q
    st.session_state.pending_q = None

    st.session_state.messages.append({'role': 'user', 'content': question})
    with st.chat_message('user', avatar='🧑‍🎓'):
        st.markdown(question)

    with st.chat_message('assistant', avatar='🔍'):
        answer = None
        err_msg = None
        with st.spinner("Thinking..."):
            ctx = build_context(question)
            try:
                answer = ask_llm(question, ctx)
            except Exception as e:
                err_msg = str(e)[:120]

        if answer:
            st.write_stream(stream_text(answer))
            st.session_state.messages.append(
                {'role': 'assistant', 'content': answer}
            )
        elif err_msg:
            err = f'Sorry, I had trouble answering that. ({err_msg})'
            st.error(err)
            st.session_state.messages.append({'role': 'assistant', 'content': err})
        else:
            fallback = "I wasn't able to generate a response. Please try again."
            st.warning(fallback)
            st.session_state.messages.append(
                {'role': 'assistant', 'content': fallback}
            )
