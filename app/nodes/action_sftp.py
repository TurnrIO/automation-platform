"""SFTP/FTP file transfer action node."""
import io
import stat as _stat
import json
from app.nodes._utils import _render

NODE_TYPE = "action.sftp"
LABEL = "SFTP / FTP"


# ── helpers ───────────────────────────────────────────────────────────────

def _sftp_walk(sftp, path, depth=0):
    """Recursively list an SFTP directory tree. Returns a flat list."""
    results = []
    try:
        attrs = sftp.listdir_attr(path)
    except Exception:
        return results
    for a in attrs:
        full_path = path.rstrip('/') + '/' + a.filename
        is_dir = _stat.S_ISDIR(a.st_mode or 0)
        results.append({
            'name':   a.filename,
            'path':   full_path,
            'size':   a.st_size,
            'is_dir': is_dir,
            'depth':  depth,
        })
        if is_dir:
            results.extend(_sftp_walk(sftp, full_path, depth + 1))
    return results


def _ftp_walk(ftp, path, depth=0):
    """Recursively list an FTP directory tree using MLSD (with nlst fallback)."""
    results = []
    try:
        entries = list(ftp.mlsd(path))
        for name, facts in entries:
            if name in ('.', '..'):
                continue
            full_path = path.rstrip('/') + '/' + name
            is_dir = facts.get('type', '').lower() in ('dir', 'cdir', 'pdir')
            results.append({
                'name':   name,
                'path':   full_path,
                'size':   int(facts.get('size', 0) or 0),
                'is_dir': is_dir,
                'depth':  depth,
            })
            if is_dir:
                results.extend(_ftp_walk(ftp, full_path, depth + 1))
    except Exception:
        # Fallback: nlst + cwd trick to detect directories
        try:
            names = ftp.nlst(path)
            for full_path in names:
                name = full_path.rstrip('/').split('/')[-1]
                if not name or name in ('.', '..'):
                    continue
                is_dir = False
                try:
                    orig = ftp.pwd()
                    ftp.cwd(full_path)
                    ftp.cwd(orig)
                    is_dir = True
                except Exception:
                    pass
                results.append({
                    'name':   name,
                    'path':   full_path,
                    'size':   0,
                    'is_dir': is_dir,
                    'depth':  depth,
                })
                if is_dir:
                    results.extend(_ftp_walk(ftp, full_path, depth + 1))
        except Exception:
            pass
    return results


def _ftp_list_flat(ftp, path):
    """Structured flat listing for FTP using MLSD (with nlst fallback)."""
    results = []
    try:
        for name, facts in ftp.mlsd(path):
            if name in ('.', '..'):
                continue
            full_path = path.rstrip('/') + '/' + name
            is_dir = facts.get('type', '').lower() in ('dir', 'cdir', 'pdir')
            results.append({
                'name':   name,
                'path':   full_path,
                'size':   int(facts.get('size', 0) or 0),
                'is_dir': is_dir,
                'depth':  0,
            })
    except Exception:
        # Fallback: nlst only
        try:
            for full_path in ftp.nlst(path):
                name = full_path.rstrip('/').split('/')[-1]
                if not name or name in ('.', '..'):
                    continue
                is_dir = False
                try:
                    orig = ftp.pwd()
                    ftp.cwd(full_path)
                    ftp.cwd(orig)
                    is_dir = True
                except Exception:
                    pass
                results.append({
                    'name':   name,
                    'path':   full_path,
                    'size':   0,
                    'is_dir': is_dir,
                    'depth':  0,
                })
        except Exception:
            pass
    return results


# ── main ──────────────────────────────────────────────────────────────────

def run(config, inp, context, logger, creds=None, **kwargs):
    """Transfer files and manage paths via SFTP or FTP.

    Operations: list, upload, download, delete, mkdir, rename, exists, stat
    """
    protocol    = (config.get('protocol', 'sftp') or 'sftp').lower()
    host        = _render(config.get('host', ''), context, creds)
    port_str    = _render(config.get('port', ''), context, creds)
    username    = _render(config.get('username', ''), context, creds)
    password    = _render(config.get('password', ''), context, creds)
    operation   = (config.get('operation', 'list') or 'list').lower()
    remote_path = _render(config.get('remote_path', '/'), context, creds)
    new_path    = _render(config.get('new_path', ''), context, creds)   # for rename
    content     = _render(config.get('content', ''), context, creds)
    recursive   = str(config.get('recursive', 'false')).lower() in ('true', '1', 'yes')
    timeout     = int(config.get('timeout', 30) or 30)

    # ── credential shortcut ───────────────────────────────────────────────
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                if not config.get('protocol'):
                    protocol = (c.get('protocol', protocol) or protocol).lower()
                host     = host     or c.get('host', '')
                port_str = port_str or str(c.get('port', ''))
                username = username or c.get('username', '')
                password = password or c.get('password', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    default_port = 22 if protocol == 'sftp' else 21
    port = int(port_str or default_port)

    if not host:
        raise ValueError("SFTP node: no host configured")

    # ══════════════════════════════════════════════════════════════════════
    # SFTP
    # ══════════════════════════════════════════════════════════════════════
    if protocol == 'sftp':
        import paramiko

        transport = paramiko.Transport((host, port))
        transport.banner_timeout  = timeout
        transport.handshake_timeout = timeout
        try:
            transport.connect(username=username or None, password=password or None)
            sftp = paramiko.SFTPClient.from_transport(transport)
            try:

                # ── list ──────────────────────────────────────────────────
                if operation == 'list':
                    if recursive:
                        files = _sftp_walk(sftp, remote_path)
                        dirs  = [f for f in files if f['is_dir']]
                        return {
                            'files': files,
                            'count': len(files),
                            'dir_count':  len(dirs),
                            'file_count': len(files) - len(dirs),
                            'path':      remote_path,
                            'recursive': True,
                        }
                    else:
                        attrs = sftp.listdir_attr(remote_path)
                        files = [{
                            'name':   a.filename,
                            'path':   remote_path.rstrip('/') + '/' + a.filename,
                            'size':   a.st_size,
                            'is_dir': _stat.S_ISDIR(a.st_mode or 0),
                            'depth':  0,
                        } for a in attrs]
                        return {'files': files, 'count': len(files), 'path': remote_path, 'recursive': False}

                # ── upload ────────────────────────────────────────────────
                elif operation == 'upload':
                    data = content.encode('utf-8') if isinstance(content, str) else content
                    sftp.putfo(io.BytesIO(data), remote_path)
                    return {'uploaded': True, 'remote_path': remote_path, 'size': len(data)}

                # ── download ──────────────────────────────────────────────
                elif operation == 'download':
                    buf = io.BytesIO()
                    sftp.getfo(remote_path, buf)
                    text = buf.getvalue().decode('utf-8', errors='replace')
                    return {'content': text, 'remote_path': remote_path, 'size': len(text)}

                # ── delete ────────────────────────────────────────────────
                elif operation == 'delete':
                    sftp.remove(remote_path)
                    return {'deleted': True, 'remote_path': remote_path}

                # ── mkdir ─────────────────────────────────────────────────
                elif operation == 'mkdir':
                    sftp.mkdir(remote_path)
                    return {'created': True, 'remote_path': remote_path}

                # ── rename / move ─────────────────────────────────────────
                elif operation == 'rename':
                    if not new_path:
                        raise ValueError("SFTP rename: 'new_path' is required")
                    sftp.rename(remote_path, new_path)
                    return {'renamed': True, 'from': remote_path, 'to': new_path}

                # ── exists ────────────────────────────────────────────────
                elif operation == 'exists':
                    try:
                        a = sftp.stat(remote_path)
                        is_dir = _stat.S_ISDIR(a.st_mode or 0)
                        return {'exists': True, 'is_dir': is_dir, 'path': remote_path}
                    except FileNotFoundError:
                        return {'exists': False, 'is_dir': False, 'path': remote_path}

                # ── stat ──────────────────────────────────────────────────
                elif operation == 'stat':
                    import datetime
                    a = sftp.stat(remote_path)
                    is_dir = _stat.S_ISDIR(a.st_mode or 0)
                    mtime = datetime.datetime.utcfromtimestamp(a.st_mtime).isoformat() + 'Z' if a.st_mtime else None
                    return {
                        'path':     remote_path,
                        'size':     a.st_size,
                        'is_dir':   is_dir,
                        'modified': mtime,
                        'mode':     oct(a.st_mode) if a.st_mode else None,
                    }

                else:
                    raise ValueError(
                        f"SFTP: unknown operation '{operation}'. "
                        "Use: list, upload, download, delete, mkdir, rename, exists, stat"
                    )

            finally:
                sftp.close()
        finally:
            transport.close()

    # ══════════════════════════════════════════════════════════════════════
    # FTP
    # ══════════════════════════════════════════════════════════════════════
    elif protocol == 'ftp':
        import ftplib

        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=timeout)
        try:
            ftp.login(username or '', password or '')

            # ── list ──────────────────────────────────────────────────────
            if operation == 'list':
                if recursive:
                    files = _ftp_walk(ftp, remote_path)
                    dirs  = [f for f in files if f['is_dir']]
                    return {
                        'files': files,
                        'count': len(files),
                        'dir_count':  len(dirs),
                        'file_count': len(files) - len(dirs),
                        'path':      remote_path,
                        'recursive': True,
                    }
                else:
                    files = _ftp_list_flat(ftp, remote_path)
                    return {'files': files, 'count': len(files), 'path': remote_path, 'recursive': False}

            # ── upload ────────────────────────────────────────────────────
            elif operation == 'upload':
                data = content.encode('utf-8') if isinstance(content, str) else content
                ftp.storbinary(f'STOR {remote_path}', io.BytesIO(data))
                return {'uploaded': True, 'remote_path': remote_path}

            # ── download ──────────────────────────────────────────────────
            elif operation == 'download':
                buf = io.BytesIO()
                ftp.retrbinary(f'RETR {remote_path}', buf.write)
                text = buf.getvalue().decode('utf-8', errors='replace')
                return {'content': text, 'remote_path': remote_path, 'size': len(text)}

            # ── delete ────────────────────────────────────────────────────
            elif operation == 'delete':
                ftp.delete(remote_path)
                return {'deleted': True, 'remote_path': remote_path}

            # ── mkdir ─────────────────────────────────────────────────────
            elif operation == 'mkdir':
                ftp.mkd(remote_path)
                return {'created': True, 'remote_path': remote_path}

            # ── rename / move ─────────────────────────────────────────────
            elif operation == 'rename':
                if not new_path:
                    raise ValueError("FTP rename: 'new_path' is required")
                ftp.rename(remote_path, new_path)
                return {'renamed': True, 'from': remote_path, 'to': new_path}

            # ── exists ────────────────────────────────────────────────────
            elif operation == 'exists':
                is_dir = False
                found  = False
                try:
                    # Try MLST (most reliable)
                    resp = ftp.sendcmd(f'MLST {remote_path}')
                    found  = True
                    is_dir = 'type=dir' in resp.lower()
                except ftplib.error_perm:
                    # Try SIZE (works for files)
                    try:
                        ftp.size(remote_path)
                        found = True
                    except ftplib.error_perm:
                        pass
                    # Try CWD (works for directories)
                    if not found:
                        try:
                            orig = ftp.pwd()
                            ftp.cwd(remote_path)
                            ftp.cwd(orig)
                            found  = True
                            is_dir = True
                        except ftplib.error_perm:
                            pass
                return {'exists': found, 'is_dir': is_dir, 'path': remote_path}

            # ── stat ──────────────────────────────────────────────────────
            elif operation == 'stat':
                size   = None
                mtime  = None
                is_dir = False
                try:
                    resp = ftp.sendcmd(f'MLST {remote_path}')
                    is_dir = 'type=dir' in resp.lower()
                    for part in resp.split(';'):
                        part = part.strip()
                        if part.lower().startswith('size='):
                            size = int(part.split('=')[1])
                        elif part.lower().startswith('modify='):
                            raw = part.split('=')[1].strip()
                            # YYYYMMDDHHMMSS[.sss]
                            import datetime
                            mtime = datetime.datetime.strptime(raw[:14], '%Y%m%d%H%M%S').isoformat() + 'Z'
                except ftplib.error_perm:
                    try:
                        size = ftp.size(remote_path)
                    except Exception:
                        pass
                return {'path': remote_path, 'size': size, 'is_dir': is_dir, 'modified': mtime}

            else:
                raise ValueError(
                    f"FTP: unknown operation '{operation}'. "
                    "Use: list, upload, download, delete, mkdir, rename, exists, stat"
                )

        finally:
            try:
                ftp.quit()
            except Exception:
                pass

    else:
        raise ValueError(f"SFTP node: unknown protocol '{protocol}'. Use sftp or ftp")
