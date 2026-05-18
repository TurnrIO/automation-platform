"""Shared utilities for node modules."""
import ast
import re
import json
from json import JSONDecodeError

SAFE_BUILTINS = {'len': len, 'str': str, 'int': int, 'float': float,
                   'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple,
                   'range': range, 'abs': abs, 'min': min, 'max': max, 'round': round,
                   'sum': sum, 'any': any, 'all': all, 'sorted': sorted,
                   'isinstance': isinstance, 'issubclass': issubclass}


def _safe_eval(expr: str, local_vars: dict) -> bool:
    """Evaluate a Python expression safely using AST validation.

    The expression is parsed into an AST tree which is walked to ensure it
    contains no forbidden nodes (Attribute, Call, Subscript on dangerous
    globals, or Name nodes referring to dangerous builtins).  If validation
    passes the expression is compiled and evaluated with SAFE_BUILTINS
    plus the supplied local_vars.

    Returns the expression result.  Raises ValueError on invalid / unsafe
    expressions; returns the local_var value for other eval errors.
    """
    try:
        tree = ast.parse(expr, mode='eval')
    except SyntaxError:
        raise ValueError(f"Invalid expression syntax: {expr!r}")

    # Nodes that are always allowed (Call is in here but validated specially in _check)
    ALLOWED_AST_NODES = {
        ast.Expression, ast.Module,
        # Literals
        ast.Constant, ast.List, ast.Dict, ast.Set, ast.Tuple,
        # Operators
        ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare,
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
        ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
        ast.Is, ast.IsNot, ast.In, ast.NotIn,
        ast.And, ast.Or, ast.Not,
        ast.Invert, ast.UAdd, ast.USub,
        # Sequences / slicing
        ast.Index, ast.Slice, ast.ExtSlice,
        # Variables / attributes / subscripts — checked below
        ast.Name, ast.Attribute, ast.Subscript,
        # Other
        ast.IfExp,  # ternary  x if cond else y
        ast.Starred,
        # Contexts (yielded by ast.walk())
        ast.Load, ast.Store, ast.Del,
        # Calls — validated more precisely in _check below
        ast.Call,
    }

    # Builtin names that are NOT allowed (too powerful)
    FORBIDDEN_BUILTINS = {
        'eval', 'exec', 'compile', 'open', 'file', 'type', 'object',
        'memoryview', 'classmethod', 'staticmethod', 'property',
        'vars', 'globals', 'locals', 'dir', 'help', 'copyright',
        'exit', 'quit', 'breakpoint', 'reload', '__import__',
        # IMP-F16: also block dynamic attribute access / reflection
        'getattr', 'setattr', 'delattr', 'hasattr',
    }

    def _check(node, allowed: bool = False):
        """Walk AST node. Return True if safe, False to skip branch."""
        node_type = type(node)
        if node_type not in ALLOWED_AST_NODES:
            raise ValueError(f"Unsafe or unsupported expression element: {node_type.__name__}")
        if node_type is ast.Name:
            if node.id in FORBIDDEN_BUILTINS:
                raise ValueError(f"Forbidden builtin: {node.id}")
        if node_type is ast.Attribute:
            # Disallow attribute access on the 'json' module or other suspicious globals
            if isinstance(node.value, ast.Name) and node.value.id in ('json', 'os', 'sys', 'subprocess', 'builtins'):
                raise ValueError(f"Attribute access on '{node.value.id}' is not allowed")
        if node_type is ast.Call:
            # Block direct calls to dangerous builtins by name (e.g. eval(source))
            if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_BUILTINS:
                raise ValueError(f"Forbidden builtin call: {node.func.id}")
            # Allow method calls on known-safe local variable types (dict, list, str, int, etc.)
            # e.g. input.get('x'), item.upper(), mylist.append(v) — safe as long as the
            # local variable itself is not a dangerous object.
            if isinstance(node.func, ast.Attribute):
                base = node.func.value
                # Allow if base is a known-safe type name (str, int, list, dict, etc.)
                # e.g. str(obj), int(s), len(lst) — these are type constructors
                if isinstance(base, ast.Name) and base.id in SAFE_BUILTINS:
                    pass  # allowed: str(obj), int(s), len(x), isinstance(...)
                # Allow if base is a subscript on a safe type (e.g. locals_dict['key'].method())
                # This is already covered by the attribute access check above on node.func
                else:
                    # For any other method call, check the attribute name isn't dangerous
                    attr = node.func.attr
                    if attr in (
                        '__class__', '__bases__', '__base__', '__mro__', '__subclasses__',
                        '__init__', '__globals__', '__code__', '__closure__', '__func__',
                        '__getattribute__', '__getattr__', '__setattr__', '__delattr__',
                    ):
                        raise ValueError(f"Forbidden attribute/method: {attr}")
            # Block dynamic function calls (e.g. (lambda: os.system)())
            if isinstance(node.func, (ast.BinOp, ast.Subscript, ast.BoolOp)):
                raise ValueError("Dynamic function calls are not allowed")
        if node_type is ast.Subscript:
            # Disallow subscript on dangerous builtins like builtins.open
            if isinstance(node.value, ast.Name) and node.value.id in ('builtins',):
                raise ValueError(f"Subscript on '{node.value.id}' is not allowed")
        return True

    for child in ast.walk(tree):
        _check(child)

    try:
        code = compile(tree, '<expr>', 'eval')
        return eval(code, {"__builtins__": SAFE_BUILTINS}, local_vars)
    except (TypeError, NameError, KeyError, IndexError, AttributeError, ValueError):
        # Re-raise our own ValueErrors from the check above
        raise
    except (ArithmeticError, OSError, RuntimeError) as e:
        raise ValueError(f"Expression evaluation error: {e}")


_TMPL = re.compile(r'\{\{([^}]+)\}\}')


def _resolve_cred_raw(cred_name: str, creds: dict):
    """Return the raw credential string for *cred_name*.

    Handles two calling conventions:
    - ``cred_name`` is a plain name ("my-smtp")  → looked up in *creds*
    - ``cred_name`` was produced by rendering ``{{creds.my-smtp}}`` and is
      therefore already the raw JSON/secret string → returned as-is.

    This prevents a silent fallback to env-var defaults when users
    type ``{{creds.name}}`` in the "Credential (name)" node field instead
    of the bare name ``name``.
    """
    if not cred_name or not creds:
        return None
    raw = creds.get(cred_name)
    if raw is None and cred_name.lstrip().startswith('{'):
        # Already resolved by _render — use the JSON value directly
        raw = cred_name
    return raw


def _render(text: str, ctx: dict, creds: dict = None, predecessor_ids=None) -> str:
    """
    Render template variables in text.
    Supports:
    - {{node_id.field}} — reference output of a previous node
    - {{creds.name}} or {{creds.name.field}} — credential vault access

    Args:
        predecessor_ids: set of node IDs that are actual predecessors of the
            current node in the execution graph. Node references outside this
            set are treated as unsafe and return the placeholder unchanged,
            preventing cross-node data exfiltration via template injection.
            When None (default), no graph-based restriction is applied
            (backward-compatible for direct callers not invoked via executor).
    """
    if not isinstance(text, str):
        return text

    def replace(m):
        key = m.group(1).strip()

        # Credential vault: {{creds.name}} or {{creds.name.field}}
        if key.startswith('creds.') and creds is not None:
            rest  = key[6:].strip()
            parts = rest.split('.', 1)
            cred_name  = parts[0]
            cred_field = parts[1] if len(parts) > 1 else None
            raw = creds.get(cred_name)
            if raw is None:
                return m.group(0)
            if cred_field:
                try:
                    data = json.loads(raw)
                    val  = data.get(cred_field)
                    return str(val) if val is not None else m.group(0)
                except (JSONDecodeError, AttributeError):
                    return m.group(0)
            return raw

        # Context reference: {{node_id.field}} or {{node_id}}
        parts = key.split('.', 1)
        node_id = parts[0]
        field   = parts[1] if len(parts) > 1 else None

        # Guard: only allow references to actual predecessors in the graph
        # when predecessor_ids is explicitly provided (via executor path)
        if predecessor_ids is not None and node_id not in predecessor_ids:
            return m.group(0)

        val = ctx.get(node_id)
        if val is None:
            return m.group(0)
        if field:
            if isinstance(val, dict):
                val = val.get(field, m.group(0))
            else:
                return m.group(0)
        return str(val) if not isinstance(val, str) else val

    return _TMPL.sub(replace, text)

