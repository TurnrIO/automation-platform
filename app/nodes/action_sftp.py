"""SFTP/FTP file transfer action node."""
import io
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.sftp"
LABEL = "SFTP / FTP"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Transfer files via SFTP or FTP."""
    protocol = (config.get('protocol', 'sftp') or 'sftp').lower()
    host = _render(config.get('host', ''), context, creds)
    port_str = _render(config.get('port', ''), context, creds)
    username = _render(config.get('username', ''), context, creds)
    password = _render(config.get('password', ''), context, creds)
    operation = (config.get('operation', 'list') or 'list').lower()
    remote_path = _render(config.get('remote_path', '/'), context, creds)
    content = _render(config.get('content', ''), context, creds)

    # Structured credential shortcut
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                # credential protocol overrides node's default only if node left it blank
                if not config.get('protocol'):
                    protocol = (c.get('protocol', protocol) or protocol).lower()
                host = host or c.get('host', '')
                port_str = port_str or str(c.get('port', ''))
                username = username or c.get('username', '')
                password = password or c.get('password', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    default_port = 22 if protocol == 'sftp' else 21
    port = int(port_str or default_port)

    if not host:
        raise ValueError("SFTP: no host configured")

    if protocol == 'sftp':
        import paramiko

        transport = paramiko.Transport((host, port))
        try:
            transport.connect(username=username or None, password=password or None)
            sftp = paramiko.SFTPClient.from_transport(transport)

            try:
                if operation == 'list':
                    attrs = sftp.listdir_attr(remote_path)
                    files = [{'name': a.filename, 'size': a.st_size,
                              'is_dir': __import__('stat').S_ISDIR(a.st_mode or 0)} for a in attrs]
                    return {'files': files, 'count': len(files), 'path': remote_path}

                elif operation == 'upload':
                    data = content.encode('utf-8') if isinstance(content, str) else content
                    sftp.putfo(io.BytesIO(data), remote_path)
                    return {'uploaded': True, 'remote_path': remote_path, 'size': len(data)}

                elif operation == 'download':
                    buf = io.BytesIO()
                    sftp.getfo(remote_path, buf)
                    text = buf.getvalue().decode('utf-8', errors='replace')
                    return {'content': text, 'remote_path': remote_path, 'size': len(text)}

                elif operation == 'delete':
                    sftp.remove(remote_path)
                    return {'deleted': True, 'remote_path': remote_path}

                elif operation == 'mkdir':
                    sftp.mkdir(remote_path)
                    return {'created': True, 'remote_path': remote_path}

                else:
                    raise ValueError(f"SFTP: unknown operation '{operation}'. Use list/upload/download/delete/mkdir")

            finally:
                sftp.close()

        finally:
            transport.close()

    elif protocol == 'ftp':
        import ftplib

        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=30)

        try:
            ftp.login(username or '', password or '')

            if operation == 'list':
                entries = []
                ftp.retrlines(f'LIST {remote_path}', entries.append)
                return {'files': entries, 'count': len(entries), 'path': remote_path}

            elif operation == 'upload':
                data = content.encode('utf-8') if isinstance(content, str) else content
                ftp.storbinary(f'STOR {remote_path}', io.BytesIO(data))
                return {'uploaded': True, 'remote_path': remote_path}

            elif operation == 'download':
                buf = io.BytesIO()
                ftp.retrbinary(f'RETR {remote_path}', buf.write)
                text = buf.getvalue().decode('utf-8', errors='replace')
                return {'content': text, 'remote_path': remote_path, 'size': len(text)}

            elif operation == 'delete':
                ftp.delete(remote_path)
                return {'deleted': True, 'remote_path': remote_path}

            elif operation == 'mkdir':
                ftp.mkd(remote_path)
                return {'created': True, 'remote_path': remote_path}

            else:
                raise ValueError(f"FTP: unknown operation '{operation}'. Use list/upload/download/delete/mkdir")

        finally:
            try:
                ftp.quit()
            except:
                pass

    else:
        raise ValueError(f"SFTP node: unknown protocol '{protocol}'. Use sftp or ftp")

