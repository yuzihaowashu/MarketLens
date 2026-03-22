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
    return None


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

    st.markdown("""
    <style>
    [data-testid="collapsedControl"] { display: none !important; }
    section[data-testid="stSidebar"]  { display: none !important; }
    header[data-testid="stHeader"]    { display: none !important; }
    .block-container { padding-top: 0 !important; padding-bottom: 0 !important; }
    [data-testid="stAppViewBlockContainer"] { padding-top: 0 !important; padding-bottom: 0 !important; }
    iframe { border: none !important; }
    </style>
    """, unsafe_allow_html=True)

    hour = datetime.datetime.now().hour
    greeting = (
        'Good morning' if hour < 12
        else ('Good afternoon' if hour < 18 else 'Good evening')
    )

    _landing = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .ml-orbit-scene *  { margin:0; padding:0; box-sizing:border-box; }
    .ml-orbit-scene    { font-family:'Inter',sans-serif; }

    .ml-orbit-scene {
        position: relative; width: 100%; height: 680px;
        display: flex; justify-content: center; align-items: center;
        overflow: visible;
    }

    /* ---- rectangular logo card ---- */
    .ml-orbit-center {
        position: relative; z-index: 10; text-align: center;
        pointer-events: none;
        padding: 2.2rem 3.4rem;
        border-radius: 18px;
        background: rgba(20, 20, 35, 0.82);
        border: 1.5px solid rgba(102, 126, 234, 0.35);
        box-shadow: 0 0 60px rgba(102, 126, 234, 0.15),
                    0 0 120px rgba(118, 75, 162, 0.08);
    }
    .ml-logo-glow {
        position: absolute; top: 50%; left: 50%;
        transform: translate(-50%,-50%);
        width: 320px; height: 200px;
        background: radial-gradient(ellipse, rgba(102,126,234,0.18) 0%, transparent 70%);
        border-radius: 50%;
        animation: mlGlowPulse 4s ease-in-out infinite;
    }
    @keyframes mlGlowPulse {
        0%,100% { transform: translate(-50%,-50%) scale(1);   opacity:.5; }
        50%     { transform: translate(-50%,-50%) scale(1.2);  opacity:1; }
    }
    .ml-logo-text {
        font-size: 2.8rem; font-weight: 700; position: relative;
        background: linear-gradient(135deg,#667eea,#764ba2);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text; color: transparent;
        letter-spacing: -0.5px;
    }
    .ml-logo-icon { font-size: 2.4rem; margin-bottom: .3rem; position: relative; }
    .ml-logo-sub {
        font-size: 1rem; color: rgba(160,160,180,0.85); margin-top: .6rem;
        font-weight: 300; position: relative;
    }

    /* ---- orbit trail (decorative ellipse ring) ---- */
    .ml-orbit-trail {
        position: absolute; top: 50%; left: 50%;
        width: 894px; height: 520px;
        transform: translate(-50%, -50%);
        border: 1px solid rgba(102, 126, 234, 0.1);
        border-radius: 50%;
        pointer-events: none;
    }

    /* ---- ellipse wrapper: stretches circle into ellipse ---- */
    .ml-orbit-ellipse {
        position: absolute; top: 50%; left: 50%;
        width: 0; height: 0;
        transform: scaleX(1.72);
    }

    .ml-orbit-ring {
        animation: mlOrbitSpin 30s linear infinite;
        will-change: transform;
    }
    @keyframes mlOrbitSpin {
        from { transform: rotate(0deg); }
        to   { transform: rotate(360deg); }
    }

    .ml-orbit-slot {
        position: absolute; width: 210px;
        margin-left: -105px; margin-top: -88px;
        top: 0; left: 0;
    }
    .ml-slot-1 { transform: translateY(-260px); }
    .ml-slot-2 { transform: rotate(120deg) translateY(-260px); }
    .ml-slot-3 { transform: rotate(240deg) translateY(-260px); }

    a.ml-planet {
        display: block; text-decoration: none; color: white;
        cursor: pointer; will-change: transform;
    }
    .ml-slot-1 a.ml-planet { animation: mlC1 30s linear infinite; }
    .ml-slot-2 a.ml-planet { animation: mlC2 30s linear infinite; }
    .ml-slot-3 a.ml-planet { animation: mlC3 30s linear infinite; }
    @keyframes mlC1 { from{transform:rotate(0deg)    scaleX(0.581)} to{transform:rotate(-360deg) scaleX(0.581)} }
    @keyframes mlC2 { from{transform:rotate(-120deg) scaleX(0.581)} to{transform:rotate(-480deg) scaleX(0.581)} }
    @keyframes mlC3 { from{transform:rotate(-240deg) scaleX(0.581)} to{transform:rotate(-600deg) scaleX(0.581)} }

    .ml-p-card {
        padding: 1.5rem 1.3rem; border-radius: 20px;
        text-align: center; color: white;
        box-shadow: 0 8px 32px rgba(0,0,0,.25);
        transition: box-shadow .25s, transform .25s;
    }
    .ml-p-card:hover {
        transform: scale(1.08);
        box-shadow: 0 16px 48px rgba(0,0,0,.4);
    }
    .ml-p-icon  { font-size: 2.2rem; margin-bottom: .5rem; }
    .ml-p-name  { font-size: 1.1rem; font-weight: 700; margin-bottom: .3rem; }
    .ml-p-tag   { font-size: .78rem; opacity: .88; line-height: 1.35; }

    .ml-grad-green  { background: linear-gradient(135deg,#43e97b,#38f9d7); }
    .ml-grad-purple { background: linear-gradient(135deg,#667eea,#764ba2); }
    .ml-grad-orange { background: linear-gradient(135deg,#f7971e,#ffd200); }
    </style>

    <div class="ml-orbit-scene">
        <div class="ml-orbit-trail"></div>

        <div class="ml-orbit-center">
            <div class="ml-logo-glow"></div>
            <div class="ml-logo-icon">🔍</div>
            <div class="ml-logo-text">MarketLens</div>
            <div class="ml-logo-sub">__GREETING__! Pick your level to get started.</div>
        </div>

        <div class="ml-orbit-ellipse">
            <div class="ml-orbit-ring">
                <div class="ml-orbit-slot ml-slot-1">
                    <a href="?level=curious" class="ml-planet">
                        <div class="ml-p-card ml-grad-green">
                            <div class="ml-p-icon">🌱</div>
                            <div class="ml-p-name">Just Curious</div>
                            <div class="ml-p-tag">Zero finance background —<br>explain like I'm five!</div>
                        </div>
                    </a>
                </div>
                <div class="ml-orbit-slot ml-slot-2">
                    <a href="?level=intermediate" class="ml-planet">
                        <div class="ml-p-card ml-grad-purple">
                            <div class="ml-p-icon">📊</div>
                            <div class="ml-p-name">Know the Basics</div>
                            <div class="ml-p-tag">I understand stocks &amp; bonds —<br>show me what's interesting.</div>
                        </div>
                    </a>
                </div>
                <div class="ml-orbit-slot ml-slot-3">
                    <a href="?level=analyst" class="ml-planet">
                        <div class="ml-p-card ml-grad-orange">
                            <div class="ml-p-icon">🎯</div>
                            <div class="ml-p-name">Financial Analyst</div>
                            <div class="ml-p-tag">Give me z-scores, signals,<br>and raw data.</div>
                        </div>
                    </a>
                </div>
            </div>
        </div>
    </div>
    """.replace("__GREETING__", greeting)

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
    st.markdown(f"### {cfg['icon']} {cfg['label']} Mode")
    if st.button('↩ Change level', use_container_width=True):
        st.session_state.level = None
        st.session_state.messages = []
        st.session_state._transitioning = True
        st.rerun()

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
                icon = '📈' if row['MAGNITUDE'] >= 0 else '📉'
                summary_text = (
                    f"{icon} **{row['ENTITY']}** {row['MAGNITUDE']:+.1f}%  \n"
                    f"{row['SUMMARY']}"
                )
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

# ── Top section: suggestions (left) + market insights (right) ────────
if cfg['show_signals']:
    _top_left, _top_right = st.columns([2.5, 1.2])
else:
    _top_left, _top_right = st.columns([1, 0.001])

with _top_right:
    if cfg['show_signals']:
        st.markdown("#### 📰 Market Insights")
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

with _top_left:
    if not st.session_state.messages:
        st.markdown('')
        st.markdown(f"##### {cfg['icon']} {cfg['tagline']}")
        st.markdown("Try one of these, or type your own question:")
        cols = st.columns(len(cfg['suggestions']))
        for i, (btn_label, full_q) in enumerate(cfg['suggestions']):
            with cols[i]:
                if st.button(btn_label, use_container_width=True, key=f'sug_{i}'):
                    st.session_state.pending_q = full_q

# ── Chat history ─────────────────────────────────────────────────────
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
