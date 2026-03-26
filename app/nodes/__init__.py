"""
Node registry — auto-discovers all node modules in this package.

Each module must export:
  NODE_TYPE : str   — e.g. "action.http_request"
  run(config, inp, context, logger, creds=None, **kwargs) -> dict

Drop a .py file in app/nodes/custom/ to add a custom node with zero restart.
"""
import importlib
import pkgutil
import logging
from pathlib import Path

log = logging.getLogger(__name__)
_registry: dict = {}


def _load_package(package_name: str, path: str):
    """Load all node modules from a package directory."""
    for mod_info in pkgutil.iter_modules([path]):
        if mod_info.name.startswith('_'):
            continue
        full_name = f"{package_name}.{mod_info.name}"
        try:
            mod = importlib.import_module(full_name)
            if hasattr(mod, 'NODE_TYPE') and hasattr(mod, 'run'):
                _registry[mod.NODE_TYPE] = mod.run
                log.debug(f"Registered node: {mod.NODE_TYPE}")
        except Exception as e:
            log.warning(f"Failed to load node module {full_name}: {e}")


# Load built-in nodes
_load_package(__name__, str(Path(__file__).parent))

# Load custom nodes (app/nodes/custom/)
_custom_path = Path(__file__).parent / 'custom'
_custom_path.mkdir(exist_ok=True)
_load_package(f"{__name__}.custom", str(_custom_path))


def get_handler(node_type: str):
    """Return the run() function for a node type, or None if not found."""
    return _registry.get(node_type)


def list_node_types() -> list:
    """Return sorted list of all registered node types."""
    return sorted(_registry.keys())


def reload_custom():
    """Hot-reload custom nodes without restarting (call from admin API)."""
    _custom_path = Path(__file__).parent / 'custom'
    _load_package(f"{__name__}.custom", str(_custom_path))

