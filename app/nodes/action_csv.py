"""CSV parse / generate action node.

Operations:
  parse    — convert a CSV string to a list of dicts (header row used as keys)
  generate — convert a list of dicts (or list of lists) to a CSV string

Output (parse):    { rows: [...], count: N, headers: [...] }
Output (generate): { csv: "...", count: N }
"""

import csv
import io
import logging
from json import JSONDecodeError
from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.csv"
LABEL = "CSV"


def run(config, inp, context, logger, creds=None, **kwargs):
    operation = config.get("operation", "parse")
    logger.info("action.csv: op=%s", operation)
    delimiter = _render(config.get("delimiter", ","), context, creds) or ","
    delimiter = delimiter[0]  # ensure single char

    if operation == "parse":
        # Accept content from config field, or from upstream input
        content = _render(config.get("content", ""), context, creds)
        if not content:
            # Try upstream input
            if isinstance(inp, str):
                content = inp
            elif isinstance(inp, dict):
                field = config.get("field", "")
                content = inp.get(field, inp.get("content", inp.get("csv", "")))

        if not content:
            raise ValueError("CSV parse: no content provided")

        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
        rows = [dict(row) for row in reader]
        headers = list(reader.fieldnames or [])
        logger.info("CSV parsed %s rows, %s columns", len(rows), len(headers))
        return {"rows": rows, "count": len(rows), "headers": headers}

    elif operation == "generate":
        # Source items from config field reference or input
        field = config.get("field", "")
        if field:
            items = _render(field, context, creds)
            if isinstance(items, str):
                # Template rendered to a string — evaluate it
                try:
                    import json as _json

                    items = _json.loads(items)
                except JSONDecodeError:
                    items = inp
        else:
            items = inp

        if isinstance(items, dict):
            # Single dict → one-row CSV
            items = [items]
        if not isinstance(items, list) or not items:
            raise ValueError("CSV generate: expected a non-empty list")

        buf = io.StringIO()
        if isinstance(items[0], dict):
            headers = list(items[0].keys())
            writer = csv.DictWriter(
                buf, fieldnames=headers, delimiter=delimiter, extrasaction="ignore", lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(items)
        else:
            writer = csv.writer(buf, delimiter=delimiter, lineterminator="\n")
            writer.writerows(items)

        csv_str = buf.getvalue()
        logger.info("CSV generated %s rows", len(items))
        return {"csv": csv_str, "count": len(items)}
    else:
        raise ValueError(f"action.csv: unknown operation {operation!r}")
