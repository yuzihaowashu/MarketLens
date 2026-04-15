"""
SEC EDGAR producer for MarketLens.

Three-stage pipeline:
  1. fetch_filing_metadata  — submissions JSON   → RAW_SEC_FILINGS
  2. fetch_filing_text      — primary doc HTML   → RAW_SEC_FILING_TEXT
  3. summarize_filings      — Cortex LLM         → SEC_FILING_SUMMARIES

Each stage is independently callable (DAG task, CLI harness, or inline REPL).
Per-filing failures are caught and logged to SEC_INGEST_ERRORS so one bad
filing doesn't halt the task.

EDGAR requires a User-Agent identifying the caller (name + email). Set
SEC_USER_AGENT in .env.

CLI:
    python -m ingestion.sec_producer --ticker AAPL --stage meta
    python -m ingestion.sec_producer --stage text --limit 2
    python -m ingestion.sec_producer --stage summary --limit 2 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import config as cfg
from ingestion.sec_ticker_map import TICKER_TO_CIK, cik_for

logger = logging.getLogger(__name__)

EDGAR_SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{cik}.json'
EDGAR_ARCHIVE_URL = 'https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_doc}'

SECTION_PATTERNS = [
    ('risk',     re.compile(r'item\s*1a[\.\s]+risk\s*factors', re.I)),
    ('mdna',     re.compile(r"item\s*[27][\.\s]+management['\u2019]s\s+discussion", re.I)),
    ('business', re.compile(r'item\s*1[\.\s]+business', re.I)),
]

_NEXT_SECTION = re.compile(r'\bitem\s*\d+[a-z]?[\.\s]', re.I)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SECFiling:
    cik: str
    ticker: Optional[str]
    company_name: Optional[str]
    accession_number: str
    form_type: str
    filing_date: Optional[date]
    report_date: Optional[date]
    primary_doc_url: str
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class SECFilingText:
    accession_number: str
    section: str
    chunk_ix: int
    content: str

    @property
    def char_count(self) -> int:
        return len(self.content)


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class SECProducer:
    name = 'sec_edgar'

    def __init__(self, user_agent: Optional[str] = None, timeout: int = 30):
        self.user_agent = user_agent or cfg.SEC_USER_AGENT
        self.timeout = timeout
        self.session = requests.Session()
        if self.user_agent:
            self.session.headers.update({
                'User-Agent': self.user_agent,
                'Accept': 'application/json, text/html',
            })
        self._last_request_at = 0.0

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _require_user_agent(self) -> None:
        if not self.user_agent:
            raise RuntimeError(
                'SEC_USER_AGENT is not set. EDGAR requires a User-Agent '
                'header of the form "Name email@domain".'
            )

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_at
        if elapsed < cfg.SEC_REQUEST_SLEEP:
            time.sleep(cfg.SEC_REQUEST_SLEEP - elapsed)
        self._last_request_at = time.time()

    def _get(self, url: str) -> requests.Response:
        self._throttle()
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Stage 1: metadata
    # ------------------------------------------------------------------

    def _parse_submissions(self, cik: str, ticker: str,
                           payload: dict) -> List[SECFiling]:
        company_name = payload.get('name')
        recent = payload.get('filings', {}).get('recent', {})
        forms    = recent.get('form', []) or []
        accnums  = recent.get('accessionNumber', []) or []
        fdates   = recent.get('filingDate', []) or []
        rdates   = recent.get('reportDate', []) or []
        pdocs    = recent.get('primaryDocument', []) or []

        want = {f.upper() for f in cfg.SEC_FORMS}
        out: List[SECFiling] = []
        for i, form in enumerate(forms):
            if form.upper() not in want:
                continue
            acc = accnums[i] if i < len(accnums) else ''
            if not acc:
                continue
            primary = pdocs[i] if i < len(pdocs) else ''
            url = EDGAR_ARCHIVE_URL.format(
                cik_int=int(cik),
                accession_nodash=acc.replace('-', ''),
                primary_doc=primary,
            )
            out.append(SECFiling(
                cik=cik,
                ticker=ticker,
                company_name=company_name,
                accession_number=acc,
                form_type=form,
                filing_date=_parse_date(fdates[i] if i < len(fdates) else None),
                report_date=_parse_date(rdates[i] if i < len(rdates) else None),
                primary_doc_url=url,
            ))
        return out

    def fetch_filing_metadata(self, tickers: List[str], conn) -> int:
        """Discover recent 10-K/10-Q filings for each watchlist ticker and MERGE
        into RAW_SEC_FILINGS. Returns total rows written."""
        self._require_user_agent()
        all_filings: List[SECFiling] = []

        for ticker in tickers:
            cik = cik_for(ticker)
            if not cik:
                logger.info('[SEC.meta] ticker=%s no CIK mapping — skipping', ticker)
                continue
            try:
                url = EDGAR_SUBMISSIONS_URL.format(cik=cik)
                t0 = time.time()
                resp = self._get(url)
                payload = resp.json()
                filings = self._parse_submissions(cik, ticker, payload)
                logger.info(
                    '[SEC.meta] ticker=%s cik=%s fetched=%d elapsed_ms=%d',
                    ticker, cik, len(filings), int((time.time() - t0) * 1000),
                )
                all_filings.extend(filings)
            except Exception as exc:
                logger.warning('[SEC.meta] ticker=%s failed: %s', ticker, exc)
                _record_error(conn, None, 'meta', str(exc),
                              getattr(exc, 'response', None))

        if not all_filings:
            logger.warning('[SEC.meta] no filings to merge')
            return 0

        if cfg.SEC_DEBUG:
            logger.info('[SEC.meta] DEBUG dry-run: would merge %d filings',
                        len(all_filings))
            return 0

        return _merge_filings(conn, all_filings)

    # ------------------------------------------------------------------
    # Stage 2: text
    # ------------------------------------------------------------------

    def fetch_filing_text(self, conn, max_filings: Optional[int] = None) -> int:
        """Pull primary document HTML for filings with TEXT_INGESTED_AT IS NULL,
        clean + section-detect + chunk, then MERGE into RAW_SEC_FILING_TEXT.
        Returns total chunks written."""
        self._require_user_agent()
        limit = max_filings or cfg.SEC_MAX_FILINGS_PER_RUN

        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                SELECT CIK, ACCESSION_NUMBER, FORM_TYPE, PRIMARY_DOC_URL
                FROM SCORPION_DB.MARKETLENS.RAW_SEC_FILINGS
                WHERE TEXT_INGESTED_AT IS NULL
                  AND PRIMARY_DOC_URL IS NOT NULL
                ORDER BY FILING_DATE DESC
                LIMIT {int(limit)}
            """)
            pending = cursor.fetchall()
        finally:
            cursor.close()

        logger.info('[SEC.text] %d filings pending text ingestion', len(pending))
        total_chunks = 0

        for cik, accession, form, url in pending:
            t0 = time.time()
            try:
                resp = self._get(url)
                html = resp.text
                if cfg.SEC_DEBUG:
                    _dump_debug_html(accession, html)
                sections = extract_sections(html)
                chunks = chunk_sections(sections, cfg.SEC_CHUNK_CHARS, accession)
                logger.info(
                    '[SEC.text] accession=%s form=%s html_bytes=%d sections={%s} chunks=%d elapsed_ms=%d',
                    accession, form, len(html),
                    ', '.join(f'{k}:{v}' for k, v in _section_counts(chunks).items()),
                    len(chunks), int((time.time() - t0) * 1000),
                )
                if chunks and not cfg.SEC_DEBUG:
                    _merge_filing_text(conn, chunks)
                    _mark_text_ingested(conn, cik, accession)
                total_chunks += len(chunks)
            except Exception as exc:
                logger.warning('[SEC.text] accession=%s failed: %s', accession, exc)
                _record_error(conn, accession, 'text', str(exc),
                              getattr(exc, 'response', None))

        return total_chunks

    # ------------------------------------------------------------------
    # Stage 3: summaries via Cortex
    # ------------------------------------------------------------------

    def summarize_filings(self, conn, model: Optional[str] = None,
                           max_filings: Optional[int] = None) -> int:
        model = model or cfg.SEC_SUMMARY_MODEL
        limit = max_filings or cfg.SEC_MAX_FILINGS_PER_RUN

        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                SELECT f.ACCESSION_NUMBER, f.TICKER, f.FORM_TYPE, f.FILING_DATE
                FROM SCORPION_DB.MARKETLENS.RAW_SEC_FILINGS f
                WHERE f.TEXT_INGESTED_AT IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM SCORPION_DB.MARKETLENS.SEC_FILING_SUMMARIES s
                      WHERE s.ACCESSION_NUMBER = f.ACCESSION_NUMBER
                  )
                ORDER BY f.FILING_DATE DESC
                LIMIT {int(limit)}
            """)
            pending = cursor.fetchall()
        finally:
            cursor.close()

        logger.info('[SEC.llm] %d filings pending summarization', len(pending))
        written = 0

        for accession, ticker, form, filing_date in pending:
            try:
                text_by_section = _load_filing_text(conn, accession)
                if not text_by_section:
                    logger.warning('[SEC.llm] accession=%s has no text rows', accession)
                    continue
                summary = _summarize_one(conn, accession, text_by_section, model)
                if cfg.SEC_DEBUG:
                    logger.info('[SEC.llm] DEBUG dry-run summary for %s: %s',
                                accession, {k: (v or '')[:80] for k, v in summary.items()})
                    continue
                _merge_summary(conn, accession, ticker, form, filing_date,
                               summary, model)
                written += 1
            except Exception as exc:
                logger.warning('[SEC.llm] accession=%s failed: %s', accession, exc)
                _record_error(conn, accession, 'summary', str(exc), None)

        return written


# ---------------------------------------------------------------------------
# Text processing (pure functions — unit-testable)
# ---------------------------------------------------------------------------

def _html_to_text(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise RuntimeError(
            'beautifulsoup4 is required — pip install beautifulsoup4 lxml'
        )
    soup = BeautifulSoup(html, 'lxml' if _has_lxml() else 'html.parser')
    for tag in soup(['script', 'style', 'table']):
        tag.decompose()
    text = soup.get_text(separator='\n')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _has_lxml() -> bool:
    try:
        import lxml  # noqa: F401
        return True
    except ImportError:
        return False


def extract_sections(html: str) -> dict[str, str]:
    """Return a dict mapping section tag → extracted text. Sections that are
    not found are omitted; whatever's left lands in 'other'."""
    text = _html_to_text(html)
    found: dict[str, tuple[int, int]] = {}   # section → (start, end-candidate)

    starts: list[tuple[int, str]] = []
    for tag, pattern in SECTION_PATTERNS:
        m = pattern.search(text)
        if m:
            starts.append((m.start(), tag))

    starts.sort()
    for i, (start, tag) in enumerate(starts):
        tail = text[start + 1:]
        m_next = _NEXT_SECTION.search(tail)
        end = start + 1 + m_next.start() if m_next else min(start + 80_000, len(text))
        found[tag] = (start, end)

    sections: dict[str, str] = {}
    for tag, (s, e) in found.items():
        body = text[s:e].strip()
        if body:
            sections[tag] = body

    if not sections:
        sections['other'] = text[:80_000]
    return sections


def chunk_sections(sections: dict[str, str], chunk_chars: int,
                   accession: str) -> List[SECFilingText]:
    out: List[SECFilingText] = []
    for section, body in sections.items():
        if not body:
            continue
        for ix, start in enumerate(range(0, len(body), chunk_chars)):
            out.append(SECFilingText(
                accession_number=accession,
                section=section,
                chunk_ix=ix,
                content=body[start:start + chunk_chars],
            ))
    return out


def _section_counts(chunks: List[SECFilingText]) -> dict[str, int]:
    d: dict[str, int] = {}
    for c in chunks:
        d[c.section] = d.get(c.section, 0) + 1
    return d


# ---------------------------------------------------------------------------
# Snowflake writes
# ---------------------------------------------------------------------------

def _merge_filings(conn, filings: List[SECFiling]) -> int:
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    df = pd.DataFrame([{
        'CIK':              f.cik,
        'TICKER':           f.ticker,
        'COMPANY_NAME':     f.company_name,
        'ACCESSION_NUMBER': f.accession_number,
        'FORM_TYPE':        f.form_type,
        'FILING_DATE':      f.filing_date,
        'REPORT_DATE':      f.report_date,
        'PRIMARY_DOC_URL':  f.primary_doc_url,
        'QUERY_ID':         f.query_id,
    } for f in filings])

    stage = f'TMP_SEC_META_{uuid.uuid4().hex[:8]}'.upper()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE SCORPION_DB.MARKETLENS.{stage} (
                CIK VARCHAR(10), TICKER VARCHAR(20), COMPANY_NAME VARCHAR(500),
                ACCESSION_NUMBER VARCHAR(25), FORM_TYPE VARCHAR(20),
                FILING_DATE DATE, REPORT_DATE DATE,
                PRIMARY_DOC_URL VARCHAR(1000), QUERY_ID VARCHAR(36)
            )
        """)
        write_pandas(conn, df, stage, database='SCORPION_DB',
                     schema='MARKETLENS', quote_identifiers=False)
        cursor.execute(f"""
            MERGE INTO SCORPION_DB.MARKETLENS.RAW_SEC_FILINGS AS tgt
            USING SCORPION_DB.MARKETLENS.{stage} AS src
              ON tgt.CIK = src.CIK AND tgt.ACCESSION_NUMBER = src.ACCESSION_NUMBER
            WHEN MATCHED THEN UPDATE SET
                TICKER = src.TICKER,
                COMPANY_NAME = src.COMPANY_NAME,
                FORM_TYPE = src.FORM_TYPE,
                FILING_DATE = src.FILING_DATE,
                REPORT_DATE = src.REPORT_DATE,
                PRIMARY_DOC_URL = src.PRIMARY_DOC_URL,
                INGESTED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (CIK, TICKER, COMPANY_NAME, ACCESSION_NUMBER, FORM_TYPE,
                 FILING_DATE, REPORT_DATE, PRIMARY_DOC_URL, QUERY_ID)
            VALUES
                (src.CIK, src.TICKER, src.COMPANY_NAME, src.ACCESSION_NUMBER,
                 src.FORM_TYPE, src.FILING_DATE, src.REPORT_DATE,
                 src.PRIMARY_DOC_URL, src.QUERY_ID)
        """)
        return len(filings)
    finally:
        cursor.close()


def _merge_filing_text(conn, chunks: List[SECFilingText]) -> None:
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            MERGE INTO SCORPION_DB.MARKETLENS.RAW_SEC_FILING_TEXT AS tgt
            USING (SELECT %s AS ACCESSION_NUMBER, %s AS SECTION,
                          %s AS CHUNK_IX, %s AS CONTENT, %s AS CHAR_COUNT) AS src
              ON tgt.ACCESSION_NUMBER = src.ACCESSION_NUMBER
             AND tgt.SECTION = src.SECTION
             AND tgt.CHUNK_IX = src.CHUNK_IX
            WHEN MATCHED THEN UPDATE SET
                CONTENT = src.CONTENT,
                CHAR_COUNT = src.CHAR_COUNT,
                INGESTED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ACCESSION_NUMBER, SECTION, CHUNK_IX, CONTENT, CHAR_COUNT)
            VALUES
                (src.ACCESSION_NUMBER, src.SECTION, src.CHUNK_IX,
                 src.CONTENT, src.CHAR_COUNT)
        """, [(c.accession_number, c.section, c.chunk_ix,
               c.content, c.char_count) for c in chunks])
    finally:
        cursor.close()


def _mark_text_ingested(conn, cik: str, accession: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE SCORPION_DB.MARKETLENS.RAW_SEC_FILINGS
            SET TEXT_INGESTED_AT = CURRENT_TIMESTAMP()
            WHERE CIK = %s AND ACCESSION_NUMBER = %s
        """, (cik, accession))
    finally:
        cursor.close()


def _load_filing_text(conn, accession: str) -> dict[str, str]:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT SECTION, CHUNK_IX, CONTENT
            FROM SCORPION_DB.MARKETLENS.RAW_SEC_FILING_TEXT
            WHERE ACCESSION_NUMBER = %s
            ORDER BY SECTION, CHUNK_IX
        """, (accession,))
        rows = cursor.fetchall()
    finally:
        cursor.close()

    by_section: dict[str, list[str]] = {}
    for section, _ix, content in rows:
        by_section.setdefault(section, []).append(content or '')
    return {k: '\n'.join(v) for k, v in by_section.items()}


def _merge_summary(conn, accession: str, ticker: str, form: str,
                   filing_date, summary: dict, model: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            MERGE INTO SCORPION_DB.MARKETLENS.SEC_FILING_SUMMARIES AS tgt
            USING (SELECT %(acc)s AS ACCESSION_NUMBER) AS src
              ON tgt.ACCESSION_NUMBER = src.ACCESSION_NUMBER
            WHEN MATCHED THEN UPDATE SET
                TICKER = %(ticker)s, FORM_TYPE = %(form)s,
                FILING_DATE = %(fdate)s,
                REVENUE_NARRATIVE = %(rev)s,
                GUIDANCE_NARRATIVE = %(guid)s,
                RISK_NARRATIVE = %(risk)s,
                MANAGEMENT_TONE = %(tone)s,
                EARNINGS_CONTEXT_SUMMARY = %(ctx)s,
                MODEL = %(model)s,
                SUMMARIZED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ACCESSION_NUMBER, TICKER, FORM_TYPE, FILING_DATE,
                 REVENUE_NARRATIVE, GUIDANCE_NARRATIVE, RISK_NARRATIVE,
                 MANAGEMENT_TONE, EARNINGS_CONTEXT_SUMMARY, MODEL)
            VALUES
                (%(acc)s, %(ticker)s, %(form)s, %(fdate)s,
                 %(rev)s, %(guid)s, %(risk)s, %(tone)s, %(ctx)s, %(model)s)
        """, {
            'acc': accession, 'ticker': ticker, 'form': form,
            'fdate': filing_date,
            'rev':   summary.get('revenue'),
            'guid':  summary.get('guidance'),
            'risk':  summary.get('risk'),
            'tone':  (summary.get('tone') or '')[:32],
            'ctx':   summary.get('context'),
            'model': model,
        })
    finally:
        cursor.close()


def _record_error(conn, accession: Optional[str], stage: str,
                  error_msg: str, response) -> None:
    try:
        http_status = getattr(response, 'status_code', None)
        snippet = ''
        if response is not None:
            try:
                snippet = (response.text or '')[:1000]
            except Exception:
                snippet = ''
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO SCORPION_DB.MARKETLENS.SEC_INGEST_ERRORS
                    (ACCESSION_NUMBER, STAGE, ERROR_MSG, HTTP_STATUS, RAW_SNIPPET)
                VALUES (%s, %s, %s, %s, %s)
            """, (accession, stage, (error_msg or '')[:2000],
                  http_status, snippet))
        finally:
            cursor.close()
    except Exception as exc:
        logger.warning('SEC_INGEST_ERRORS write failed (non-fatal): %s', exc)


# ---------------------------------------------------------------------------
# Cortex prompting
# ---------------------------------------------------------------------------

_PROMPT_TEMPLATE = """You are a financial analyst reading a {form} filing for {ticker}.
Extract narrative context from the filing section below.

Respond with EXACTLY this format, no extra prose:
REVENUE: <1-2 sentences on revenue drivers, trends, segment performance>
GUIDANCE: <1-2 sentences on forward guidance or outlook; "none given" if absent>
RISK: <1-2 sentences on the most material risks mentioned>
TONE: <one word: positive | cautious | negative | neutral>
CONTEXT: <1-2 sentences summarizing earnings/operational context>

--- FILING SECTION ({section}) ---
{body}
"""


def _summarize_one(conn, accession: str, text_by_section: dict[str, str],
                   model: str) -> dict:
    """Single Cortex call per filing using the richest available section.
    Prefer mdna → risk → business → other."""
    priority = ['mdna', 'risk', 'business', 'other']
    section, body = next(
        ((s, text_by_section[s]) for s in priority if s in text_by_section),
        (None, None),
    )
    if not body:
        return {}

    body = body[:int(cfg.SEC_CHUNK_CHARS * 1.5)]
    prompt = _PROMPT_TEMPLATE.format(
        form='SEC', ticker='the issuer', section=section, body=body,
    )

    t0 = time.time()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)',
            (model, prompt),
        )
        row = cursor.fetchone()
        out = row[0] if row else ''
    finally:
        cursor.close()
    logger.info(
        '[SEC.llm] accession=%s section=%s prompt_chars=%d model=%s elapsed_ms=%d out_chars=%d',
        accession, section, len(prompt), model,
        int((time.time() - t0) * 1000), len(out or ''),
    )
    return _parse_llm_output(out or '')


def _parse_llm_output(raw: str) -> dict:
    fields = {'revenue': None, 'guidance': None, 'risk': None,
              'tone': None, 'context': None}
    key_map = {
        'REVENUE': 'revenue', 'GUIDANCE': 'guidance', 'RISK': 'risk',
        'TONE': 'tone', 'CONTEXT': 'context',
    }
    current = None
    for line in raw.splitlines():
        m = re.match(r'^(REVENUE|GUIDANCE|RISK|TONE|CONTEXT)\s*:\s*(.*)$',
                     line.strip(), re.I)
        if m:
            current = key_map[m.group(1).upper()]
            fields[current] = m.group(2).strip()
        elif current and line.strip():
            fields[current] = (fields[current] or '') + ' ' + line.strip()
    if fields['tone']:
        fields['tone'] = fields['tone'].split()[0].lower().strip('.,')
    return fields


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def _dump_debug_html(accession: str, html: str) -> None:
    debug_dir = Path('./.sec_debug')
    debug_dir.mkdir(exist_ok=True)
    (debug_dir / f'{accession}.html').write_text(html, encoding='utf-8')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    parser = argparse.ArgumentParser(prog='ingestion.sec_producer')
    parser.add_argument('--stage', choices=['meta', 'text', 'summary'], required=True)
    parser.add_argument('--ticker', action='append', default=None,
                        help='Repeatable ticker filter for --stage meta')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.dry_run:
        cfg.SEC_DEBUG = True
        logger.info('[CLI] dry-run enabled (SEC_DEBUG=True)')

    from app.snowflake_client import get_connection
    conn = get_connection()
    producer = SECProducer()

    if args.stage == 'meta':
        tickers = args.ticker or cfg.WATCHLIST_TICKERS
        n = producer.fetch_filing_metadata(tickers, conn)
        logger.info('[CLI] meta wrote %d rows', n)
    elif args.stage == 'text':
        n = producer.fetch_filing_text(conn, max_filings=args.limit)
        logger.info('[CLI] text wrote %d chunks', n)
    elif args.stage == 'summary':
        n = producer.summarize_filings(conn, max_filings=args.limit)
        logger.info('[CLI] summary wrote %d filings', n)


if __name__ == '__main__':
    _cli()
