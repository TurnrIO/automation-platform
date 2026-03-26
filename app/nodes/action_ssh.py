"""SSH command action node."""
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.ssh"
LABEL = "SSH"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute command over SSH."""
    import paramiko

    host = _render(config.get('host', ''), context, creds)
    port_str = _render(config.get('port', ''), context, creds)
    username = _render(config.get('username', ''), context, creds)
    password = _render(config.get('password', ''), context, creds)
    command = _render(config.get('command', ''), context, creds)

    # Structured credential shortcut
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                host = host or c.get('host', '')
                port_str = port_str or str(c.get('port', ''))
                username = username or c.get('username', '')
                password = password or c.get('password', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    port = int(port_str or 22)

    if not host:
        raise ValueError("SSH: no host configured")
    if not command:
        raise ValueError("SSH: no command configured")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(host, port=port, username=username or None,
                       password=password or None, timeout=30)
        _, stdout_f, stderr_f = client.exec_command(command, timeout=60)
        exit_code = stdout_f.channel.recv_exit_status()
        out = stdout_f.read().decode('utf-8', errors='replace').strip()
        err = stderr_f.read().decode('utf-8', errors='replace').strip()
    finally:
        client.close()

    return {'stdout': out, 'stderr': err, 'exit_code': exit_code, 'success': exit_code == 0}

