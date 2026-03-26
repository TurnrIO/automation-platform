# Custom Nodes

Drop a Python file here to add a custom node. No restart required — call `POST /api/admin/reload_nodes` to hot-reload.

## Template

```python
NODE_TYPE = "action.my_custom_node"
LABEL     = "My Custom Node"

def run(config: dict, inp: dict, context: dict, logger, creds: dict = None, **kwargs) -> dict:
    # config  — node's configured fields (already template-rendered by executor)
    # inp     — output of the previous node
    # context — full run context keyed by node_id
    # logger  — call logger("message") to emit a run log line
    # creds   — dict of {name: raw_value} from the credential vault
    value = config.get("my_field", "default")
    logger(f"Custom node running with value={value}")
    return {"result": value, "input_was": inp}
```

