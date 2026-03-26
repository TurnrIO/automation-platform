"""Send email action node."""
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.nodes._utils import _render

NODE_TYPE = "action.send_email"
LABEL = "Send Email"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send email via SMTP."""
    to = _render(config.get('to', ''), context, creds)
    subject = _render(config.get('subject', ''), context, creds)
    body = _render(config.get('body', ''), context, creds)
    host = _render(config.get('smtp_host', ''), context, creds)
    user = _render(config.get('smtp_user', ''), context, creds)
    pwd = _render(config.get('smtp_pass', ''), context, creds)
    port = None

    # Structured credential shortcut: credential field contains the name of an smtp credential
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                host = host or c.get('host', '')
                port = port or c.get('port')
                user = user or c.get('user', '')
                pwd = pwd or c.get('pass', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    host = host or os.environ.get('SMTP_HOST', '')
    user = user or os.environ.get('SMTP_USER', '')
    pwd = pwd or os.environ.get('SMTP_PASS', '')
    smtp_port = int(port or 465)

    if not host:
        raise ValueError("Send Email: no SMTP host configured")

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP_SSL(host, smtp_port) as s:
        if user and pwd:
            s.login(user, pwd)
        s.sendmail(user, to, msg.as_string())

    return {'sent': True, 'to': to, 'subject': subject}

