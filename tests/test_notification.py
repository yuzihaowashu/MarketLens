"""Tests for notification layer — BaseSender, broadcast, SlackSender, EmailSender."""

import os
import sys
import unittest
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

with mock.patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'T', 'SNOWFLAKE_USER': 'T'}):
    from notification.base_sender import BaseSender, broadcast, build_signal_table
    from notification.slack_sender import SlackSender
    from notification.email_sender import EmailSender


# ---------------------------------------------------------------------------
# Stub senders for testing
# ---------------------------------------------------------------------------

class AlwaysSuccessSender(BaseSender):
    channel_name = 'success_channel'
    def send(self, title, body): return True


class AlwaysFailSender(BaseSender):
    channel_name = 'fail_channel'
    def send(self, title, body): raise RuntimeError('network error')


class ReturnsFalseSender(BaseSender):
    channel_name = 'false_channel'
    def send(self, title, body): return False


class NotConfiguredSender(BaseSender):
    channel_name = 'not_configured'
    def is_configured(self): return False
    def send(self, title, body): return True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBaseSender(unittest.TestCase):

    def test_safe_send_returns_true_on_success(self):
        s = AlwaysSuccessSender()
        self.assertTrue(s.safe_send('title', 'body'))

    def test_safe_send_returns_false_on_exception(self):
        s = AlwaysFailSender()
        # Should NOT raise
        result = s.safe_send('title', 'body')
        self.assertFalse(result)

    def test_safe_send_returns_false_when_send_returns_false(self):
        s = ReturnsFalseSender()
        self.assertFalse(s.safe_send('title', 'body'))


class TestBroadcast(unittest.TestCase):

    def test_all_success(self):
        senders = [AlwaysSuccessSender(), AlwaysSuccessSender()]
        senders[1].channel_name = 'success_channel_2'
        results = broadcast(senders, 'T', 'B')
        self.assertTrue(all(results.values()))

    def test_one_failure_does_not_block_others(self):
        senders = [AlwaysFailSender(), AlwaysSuccessSender()]
        results = broadcast(senders, 'T', 'B')
        self.assertFalse(results['fail_channel'])
        self.assertTrue(results['success_channel'])

    def test_not_configured_sender_skipped(self):
        senders = [NotConfiguredSender(), AlwaysSuccessSender()]
        results = broadcast(senders, 'T', 'B')
        self.assertFalse(results['not_configured'])
        self.assertTrue(results['success_channel'])

    def test_returns_dict_keyed_by_channel_name(self):
        senders = [AlwaysSuccessSender(), AlwaysFailSender()]
        results = broadcast(senders, 'T', 'B')
        self.assertIn('success_channel', results)
        self.assertIn('fail_channel', results)


class TestBuildSignalTable(unittest.TestCase):

    def test_empty_rows(self):
        self.assertEqual(build_signal_table([]), 'No signals today.')

    def test_non_empty_rows(self):
        rows = [
            ('2025-04-09', 'STOCK_ANOMALY', 'AAPL', 5.23, 2.85,
             'AAPL moved 5.23% (z-score: 2.85)'),
        ]
        table = build_signal_table(rows)
        self.assertIn('AAPL', table)
        self.assertIn('STOCK_ANOMALY', table)

    def test_long_summary_truncated(self):
        long_summary = 'x' * 200
        rows = [('2025-04-09', 'STOCK_ANOMALY', 'TSLA', 3.0, 2.1, long_summary)]
        table = build_signal_table(rows)
        # Summary should be truncated to 50 chars in the table
        self.assertLess(table.count('x'), 60)


class TestSlackSender(unittest.TestCase):

    def test_not_configured_when_no_url(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            sender = SlackSender(webhook_url=None)
            # Override config SLACK_WEBHOOK_URL to be None
            with mock.patch('notification.slack_sender.cfg') as mock_cfg:
                mock_cfg.SLACK_WEBHOOK_URL = None
                sender2 = SlackSender(webhook_url=None)
                self.assertFalse(sender2.is_configured())

    def test_send_posts_to_webhook(self):
        sender = SlackSender(webhook_url='https://hooks.slack.com/test')
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'ok'

        with mock.patch('requests.post', return_value=mock_response) as mock_post:
            result = sender.send('Test Title', 'Test Body')

        self.assertTrue(result)
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        self.assertEqual(call_kwargs[0][0], 'https://hooks.slack.com/test')

    def test_send_returns_false_on_non_200(self):
        sender = SlackSender(webhook_url='https://hooks.slack.com/test')
        mock_response = mock.MagicMock()
        mock_response.status_code = 500
        mock_response.text = 'error'

        with mock.patch('requests.post', return_value=mock_response):
            result = sender.send('T', 'B')

        self.assertFalse(result)

    def test_send_returns_false_on_request_exception(self):
        import requests
        sender = SlackSender(webhook_url='https://hooks.slack.com/test')

        with mock.patch('requests.post', side_effect=requests.exceptions.ConnectionError('refused')):
            result = sender.send('T', 'B')

        self.assertFalse(result)

    def test_safe_send_does_not_raise_on_exception(self):
        import requests
        sender = SlackSender(webhook_url='https://hooks.slack.com/test')
        with mock.patch('requests.post', side_effect=requests.exceptions.Timeout('timeout')):
            result = sender.safe_send('T', 'B')
        self.assertFalse(result)


class TestEmailSender(unittest.TestCase):

    def test_not_configured_without_credentials(self):
        sender = EmailSender(to_addr=None, smtp_user=None, smtp_password=None)
        with mock.patch('notification.email_sender.cfg') as mock_cfg:
            mock_cfg.ALERT_EMAIL   = None
            mock_cfg.SMTP_USER     = None
            mock_cfg.SMTP_PASSWORD = None
            mock_cfg.SMTP_HOST     = 'smtp.gmail.com'
            mock_cfg.SMTP_PORT     = 587
            sender2 = EmailSender()
            self.assertFalse(sender2.is_configured())

    def test_configured_with_all_fields(self):
        sender = EmailSender(
            to_addr='test@example.com',
            smtp_user='user@example.com',
            smtp_password='password',
        )
        self.assertTrue(sender.is_configured())

    def test_send_uses_smtp(self):
        sender = EmailSender(
            to_addr='recv@example.com',
            smtp_user='send@example.com',
            smtp_password='pw',
        )
        with mock.patch('notification.email_sender.cfg') as mock_cfg:
            mock_cfg.SMTP_HOST = 'smtp.gmail.com'
            mock_cfg.SMTP_PORT = 587
            sender._host = 'smtp.gmail.com'
            sender._port = 587

        mock_smtp = mock.MagicMock()
        mock_smtp.__enter__ = mock.MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = mock.MagicMock(return_value=False)

        with mock.patch('smtplib.SMTP', return_value=mock_smtp):
            result = sender.send('Subject', 'Body text')

        self.assertTrue(result)
        mock_smtp.sendmail.assert_called_once()

    def test_send_returns_false_on_smtp_error(self):
        import smtplib
        sender = EmailSender(
            to_addr='recv@example.com',
            smtp_user='send@example.com',
            smtp_password='pw',
        )
        with mock.patch('smtplib.SMTP', side_effect=smtplib.SMTPException('fail')):
            result = sender.send('T', 'B')
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
