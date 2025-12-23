"""
monitor.py
Logging and health check utilities for ETF Market Data Pipeline.
Supports monitoring, reporting, and reliability.
"""

import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename="data/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def log_event(event: str, level: str = "info"):
    """Log a pipeline event with specified level."""
    if level == "info":
        logging.info(event)
    elif level == "warning":
        logging.warning(event)
    elif level == "error":
        logging.error(event)
    else:
        logging.debug(event)


def log_error(error: str):
    """Log an error event."""
    logging.error(error)


def health_check() -> dict:
    """
    Simple health check for pipeline status.
    Returns dict with status and last log timestamp.
    """
    try:
        with open("data/pipeline.log", "r") as f:
            lines = f.readlines()
            last_line = lines[-1] if lines else ""
            last_time = last_line.split()[0] if last_line else None
        return {"status": "OK", "last_log": last_time}
    except Exception as e:
        return {"status": "ERROR", "details": str(e)}


if __name__ == "__main__":
    log_event("Pipeline started.")
    print("Health check:", health_check())
