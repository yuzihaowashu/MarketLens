"""Email notification sender via SMTP."""

from __future__ import annotations

import logging
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from notification.base_sender import BaseSender
import config as cfg

logger = logging.getLogger(__name__)


class EmailSender(BaseSender):
    """
    Sends notifications via SMTP (e.g. Gmail with an App Password).

    Configure via environment variables:
        ALERT_EMAIL    — recipient address
        SMTP_USER      — sender address / SMTP login
        SMTP_PASSWORD  — SMTP password or App Password
        SMTP_HOST      — default: smtp.gmail.com
        SMTP_PORT      — default: 587
    """

    channel_name = 'email'

    def __init__(
        self,
        to_addr:  Optional[str] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
    ):
        self._to       = to_addr      or cfg.ALERT_EMAIL
        self._user     = smtp_user    or cfg.SMTP_USER
        self._password = smtp_password or cfg.SMTP_PASSWORD
        self._host     = cfg.SMTP_HOST
        self._port     = cfg.SMTP_PORT

    def is_configured(self) -> bool:
        return bool(self._to and self._user and self._password)

    def send(self, title: str, body: str) -> bool:
        if not self.is_configured():
            logger.debug('Email sender not fully configured')
            return False

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'[MarketLens] {title}'
        msg['From']    = self._user
        msg['To']      = self._to

        # Plain-text part
        msg.attach(MIMEText(body, 'plain'))

        # HTML part — wrap in a simple pre block for monospace signal table
        html_body = (
            '<html><body>'
            f'<h3>{title}</h3>'
            f'<pre style="font-family:monospace;font-size:13px">{body}</pre>'
            '</body></html>'
        )
        msg.attach(MIMEText(html_body, 'html'))

        try:
            with smtplib.SMTP(self._host, self._port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.login(self._user, self._password)
                server.sendmail(self._user, self._to, msg.as_string())
            return True
        except smtplib.SMTPException as exc:
            logger.warning('Email send failed: %s', exc)
            return False
