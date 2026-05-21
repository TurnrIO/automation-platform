"""Shared SMTP send helper.

Auto-selects the connection mode based on port:
  465        → SMTP_SSL  (implicit TLS)
  587        → SMTP + STARTTLS (explicit TLS — agentmail.to, Outlook, Gmail, etc.)
  25 / other → plain SMTP (LAN relay / MTA)

The default timeout is 30 s, which is enough for slower providers like
agentmail.to.  Override with the SMTP_TIMEOUT env var.

Usage:
    from app.core.smtp import send_message
    send_message(host, port, user, pwd, from_addr, to_addr, msg.as_string())
"""
import logging
import os
import ssl
import smtplib

logger = logging.getLogger(__name__)
_DEFAULT_TIMEOUT = int(os.environ.get('SMTP_TIMEOUT', '30'))


def send_message(
    host: str,
    port: int,
    user: str,
    pwd: str,
    from_addr: str,
    to_addr: str,
    msg_str: str,
    timeout: int = _DEFAULT_TIMEOUT,
) -> None:
    """Send a pre-built RFC-2822 message string via SMTP.

    Raises smtplib.SMTPException (or subclass) on failure — callers are
    responsible for catching and logging.
    """
    ctx = ssl.create_default_context()

    if port == 587:
        # Explicit TLS / STARTTLS
        try:
            with smtplib.SMTP(host, port, timeout=timeout) as s:
                s.ehlo()
                s.starttls(context=ctx)
                s.ehlo()
                if user and pwd:
                    s.login(user, pwd)
                s.sendmail(from_addr, to_addr, msg_str)
        except smtplib.SMTPException:
            # re-raised so caller can catch and log
            raise

    elif port == 465:
        # Implicit TLS / SMTP_SSL
        try:
            with smtplib.SMTP_SSL(host, port, timeout=timeout, context=ctx) as s:
                if user and pwd:
                    s.login(user, pwd)
                s.sendmail(from_addr, to_addr, msg_str)
        except smtplib.SMTPException:
            raise

    else:
        # Port 25 or any custom port — plain SMTP (no TLS)
        try:
            with smtplib.SMTP(host, port, timeout=timeout) as s:
                if user and pwd:
                    s.login(user, pwd)
                s.sendmail(from_addr, to_addr, msg_str)
        except smtplib.SMTPException:
            raise
