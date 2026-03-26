
def do_wait(seconds: float) -> dict:
    """Block the Celery worker for `seconds` seconds."""
    t = max(0.0, min(float(seconds), 3600.0))
    time.sleep(t)
    return {"waited_seconds": t}
