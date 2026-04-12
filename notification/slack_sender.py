"""Slack notification sender via incoming webhook."""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from notification.base_sender import BaseSender
import config as cfg

logger = logging.getLogger(__name__)


class SlackSender(BaseSender):
    """
    Sends notifications to a Slack channel via an incoming webhook URL.

    Configure via environment variable:
        SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
    """

    channel_name = 'slack'

    def __init__(self, webhook_url: Optional[str] = None):
        self._webhook_url = webhook_url or cfg.SLACK_WEBHOOK_URL

    def is_configured(self) -> bool:
        return bool(self._webhook_url)

    def send(self, title: str, body: str) -> bool:
        if not self._webhook_url:
            logger.debug('Slack webhook URL not configured')
            return False

        import requests

        payload = {
            'text': f'*{title}*\n```{body}```',
            'mrkdwn': True,
        }
        try:
            resp = requests.post(
                self._webhook_url,
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200 and resp.text == 'ok':
                return True
            logger.warning('Slack returned %d: %s', resp.status_code, resp.text[:200])
            return False
        except requests.exceptions.RequestException as exc:
            logger.warning('Slack request failed: %s', exc)
            return False
