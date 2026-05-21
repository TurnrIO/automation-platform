"""Send email action node."""
import ipaddress
import logging
import os
import socket
import json
from json import JSONDecodeError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from app.nodes._utils import _render, _resolve_cred_raw
from app.core.smtp import send_message

logger = logging.getLogger(__name__)
NODE_TYPE = "action.send_email"
LABEL = "Send Email"

# ── SSRF protection ────────────────────────────────────────────────────────────


_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _blocked_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        for net in _BLOCKED_NETWORKS:
            if ip in net:
                return True
    except ValueError:
        pass
    return False


def _check_ssrf(host: str) -> None:
    """"Resolve hostname and block private/blocked destinations."""
    try:
        infos = socket.getaddrinfo(host, 0)
    except socket.gaierror:
        return  # DNS failure will be caught by SMTP connection attempt
    for family, _, _, _, sockaddr in infos:
        if family == socket.AF_INET or family == socket.AF_INET6:
            ip_str = sockaddr[0]
            if _blocked_ip(ip_str):
                raise ValueError(f"Send Email: host {host} resolves to blocked IP {ip_str}")


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send email via SMTP.

    Port selection (SMTP_PORT env var or credential 'port' field):
      465 → implicit TLS (SMTP_SSL)   — legacy default
      587 → STARTTLS                  — Gmail, Outlook, most modern providers
      25  → plain SMTP                — local relay / MTA
    """
    logger.info("Send Email: firing node")
    to      = _render(config.get('to', ''), context, creds)
    subject = _render(config.get('subject', ''), context, creds)
    body    = _render(config.get('body', ''), context, creds)
    host    = _render(config.get('smtp_host', ''), context, creds)
    user    = _render(config.get('smtp_user', ''), context, creds)
    pwd     = _render(config.get('smtp_pass', ''), context, creds)
    port    = None

    # Structured credential shortcut
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                host = host or c.get('host', '')
                port = port or c.get('port')
                user = user or c.get('user', '')
                pwd  = pwd  or c.get('pass', '')
            except (JSONDecodeError, AttributeError):
                pass

    host      = host or os.environ.get('SMTP_HOST', '')
    user      = user or os.environ.get('SMTP_USER', '')
    pwd       = pwd  or os.environ.get('SMTP_PASS', '')
    smtp_port = int(port or os.environ.get('SMTP_PORT', 587))

    if not host:
        raise ValueError("Send Email: no SMTP host configured")

    _check_ssrf(host)

    logger.info("Send Email: to=%s subject=%s", to, subject)
    from_addr = os.environ.get('SMTP_FROM', '') or user

    msg = MIMEMultipart()
    msg['From']    = from_addr
    msg['To']      = to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        send_message(host, smtp_port, user, pwd, from_addr, to, msg.as_string())
    except smtplib.SMTPException as e:
        logger.error("Send Email: SMTP error sending to %s — %s", to, e)
        raise ValueError(f"Send Email: SMTP failure — {e}") from e
    except socket.gaierror as e:
        logger.error("Send Email: DNS resolution failed for host %s — %s", host, e)
        raise ValueError(f"Send Email: DNS resolution failed for {host} — {e}") from e
    except OSError as e:
        logger.error("Send Email: connection error to %s:%s — %s", host, smtp_port, e)
        raise ValueError(f"Send Email: connection error to {host}:{smtp_port} — {e}") from e

    logger.info("Send Email: completed to=%s subject=%s", to, subject)
    return {'sent': True, 'to': to, 'subject': subject}
