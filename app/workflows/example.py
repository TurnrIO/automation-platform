from datetime import datetime, timezone

def run_example(payload: dict) -> dict:
    return {"message": "Hello from v8!", "time": datetime.now(timezone.utc).isoformat(), "echo": payload}
