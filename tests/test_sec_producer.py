"""Tests for ingestion/sec_producer.py — EDGAR fetch, section detection, chunking."""

import os
import sys
import unittest
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

with mock.patch.dict(os.environ, {
    'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T',
    'SEC_USER_AGENT': 'Test Tester test@example.com',
}):
    import config as cfg
    cfg.SEC_USER_AGENT = 'Test Tester test@example.com'
    from ingestion import sec_producer as sp


def _mock_json(payload):
    r = mock.MagicMock()
    r.raise_for_status = mock.MagicMock()
    r.json = mock.MagicMock(return_value=payload)
    return r


SUBMISSIONS = {
    'name': 'Apple Inc.',
    'filings': {
        'recent': {
            'form':           ['10-K', '10-Q', '8-K', '10-Q'],
            'accessionNumber':['0000320193-24-000123',
                               '0000320193-24-000050',
                               '0000320193-24-000040',
                               '0000320193-23-000090'],
            'filingDate':     ['2024-11-01', '2024-08-02', '2024-07-15', '2023-08-03'],
            'reportDate':     ['2024-09-28', '2024-06-29', '2024-07-14', '2023-07-01'],
            'primaryDocument':['aapl-10k.htm', 'aapl-10q.htm', 'aapl-8k.htm', 'aapl-10q.htm'],
        }
    }
}


class TestParseSubmissions(unittest.TestCase):

    def test_filters_to_requested_forms(self):
        p = sp.SECProducer(user_agent='Test x@y.z')
        filings = p._parse_submissions('0000320193', 'AAPL', SUBMISSIONS)
        forms = {f.form_type for f in filings}
        self.assertEqual(forms, {'10-K', '10-Q'})
        self.assertEqual(len(filings), 3)

    def test_builds_archive_url(self):
        p = sp.SECProducer(user_agent='Test x@y.z')
        filings = p._parse_submissions('0000320193', 'AAPL', SUBMISSIONS)
        f = filings[0]
        self.assertIn('320193', f.primary_doc_url)
        self.assertIn(f.accession_number.replace('-', ''), f.primary_doc_url)
        self.assertTrue(f.primary_doc_url.endswith('.htm'))


class TestExtractSections(unittest.TestCase):

    def test_detects_risk_and_mdna(self):
        html = """
        <html><body>
        <p>Item 1. Business</p><p>We make stuff.</p>
        <p>Item 1A. Risk Factors</p><p>Markets fluctuate. Regulators act.</p>
        <p>Item 2. Management's Discussion and Analysis</p>
        <p>Revenue grew 10%. Guidance is optimistic.</p>
        <p>Item 3. Something else</p>
        </body></html>
        """
        sections = sp.extract_sections(html)
        self.assertIn('risk', sections)
        self.assertIn('mdna', sections)
        self.assertIn('Markets fluctuate', sections['risk'])
        self.assertIn('Revenue grew', sections['mdna'])

    def test_falls_back_to_other(self):
        html = "<html><body><p>just some text with no items</p></body></html>"
        sections = sp.extract_sections(html)
        self.assertEqual(list(sections.keys()), ['other'])


class TestChunkSections(unittest.TestCase):

    def test_chunk_size_and_indices(self):
        body = 'x' * 2500
        chunks = sp.chunk_sections({'risk': body}, chunk_chars=1000,
                                    accession='acc-1')
        self.assertEqual(len(chunks), 3)
        self.assertEqual([c.chunk_ix for c in chunks], [0, 1, 2])
        self.assertEqual(chunks[0].char_count, 1000)
        self.assertEqual(chunks[2].char_count, 500)
        self.assertTrue(all(c.section == 'risk' for c in chunks))


class TestParseLLMOutput(unittest.TestCase):

    def test_parses_all_fields(self):
        raw = (
            "REVENUE: Segment A grew 12% YoY.\n"
            "GUIDANCE: FY guidance raised.\n"
            "RISK: FX headwinds.\n"
            "TONE: Positive.\n"
            "CONTEXT: Strong quarter overall."
        )
        out = sp._parse_llm_output(raw)
        self.assertIn('Segment A', out['revenue'])
        self.assertEqual(out['tone'], 'positive')
        self.assertIn('Strong quarter', out['context'])

    def test_missing_fields_are_none(self):
        out = sp._parse_llm_output("REVENUE: only this\n")
        self.assertIsNotNone(out['revenue'])
        self.assertIsNone(out['guidance'])


class TestFetchMetadataHTTP(unittest.TestCase):

    def test_requires_user_agent(self):
        p = sp.SECProducer(user_agent=None)
        p.user_agent = None
        with self.assertRaises(RuntimeError):
            p.fetch_filing_metadata(['AAPL'], mock.MagicMock())

    def test_skips_unknown_ticker(self):
        p = sp.SECProducer(user_agent='T x@y.z')
        with mock.patch.object(p.session, 'get') as g:
            n = p.fetch_filing_metadata(['ZZZZ_UNKNOWN'], mock.MagicMock())
        self.assertEqual(n, 0)
        g.assert_not_called()


if __name__ == '__main__':
    unittest.main()
