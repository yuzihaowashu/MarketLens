"""
Base notification sender for MarketLens.

Inspired by daily_stock_analysis/src/notification.py:
- Each channel is an independent sender
- One channel failing never blocks others (fail-open)
- broadcast() sends to all configured channels and returns per-channel results
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List

logger = logging.getLogger(__name__)


class BaseSender(ABC):
    """
    Abstract notification sender.

    Subclasses implement send().  Callers should use safe_send() or
    broadcast() so a single broken channel doesn't abort the pipeline.
    """

    channel_name: str = 'base'

    @abstractmethod
    def send(self, title: str, body: str) -> bool:
        """
        Send a notification.

        Returns True on success, False on soft failure.
        Should NOT raise — raise only for programming errors.
        """

    def safe_send(self, title: str, body: str) -> bool:
        """
        Wraps send() so any exception is caught, logged, and returns False.
        This ensures one broken channel never propagates upward.
        """
        try:
            result = self.send(title, body)
            if result:
                logger.info('[%s] notification sent', self.channel_name)
            else:
                logger.warning('[%s] notification returned False', self.channel_name)
            return result
        except Exception as exc:
            logger.warning('[%s] notification failed: %s', self.channel_name, exc)
            return False

    def is_configured(self) -> bool:
        """Return True if this sender has valid credentials/URL."""
        return True


def broadcast(
    senders: List[BaseSender],
    title: str,
    body: str,
) -> Dict[str, bool]:
    """
    Send *title* / *body* to every configured sender.

    Returns a dict of {channel_name: success_bool} so the caller can
    log or inspect results without any single failure stopping delivery.
    """
    results: Dict[str, bool] = {}
    for sender in senders:
        if not sender.is_configured():
            logger.debug('[%s] not configured — skipping', sender.channel_name)
            results[sender.channel_name] = False
            continue
        results[sender.channel_name] = sender.safe_send(title, body)
    return results


def build_signal_table(rows) -> str:
    """
    Format V_SIGNAL_SUMMARY rows as a plain-text table for notifications.

    rows: list of tuples (DATE, SIGNAL_TYPE, ENTITY, MAGNITUDE, SALIENCE_SCORE, SUMMARY)
    """
    if not rows:
        return 'No signals today.'
    lines = ['DATE        TYPE                ENTITY         MAG    SCORE   SUMMARY',
             '-' * 90]
    for r in rows:
        date_str    = str(r[0])[:10]
        sig_type    = str(r[1])[:18].ljust(18)
        entity      = str(r[2])[:12].ljust(12)
        magnitude   = f'{float(r[3]):+.2f}' if r[3] is not None else '   N/A'
        salience    = f'{float(r[4]):.2f}'  if r[4] is not None else '  N/A'
        summary     = str(r[5])[:50]
        lines.append(f'{date_str}  {sig_type}  {entity}  {magnitude:>7}  {salience:>6}  {summary}')
    return '\n'.join(lines)
