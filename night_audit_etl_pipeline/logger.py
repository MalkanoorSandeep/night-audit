# etl/logger_setup.py

import logging
import os
from datetime import datetime
from night_audit_etl_pipeline.config_loader import config  # Adjust if config_loader is in another location

def setup_logger(name=None):
    log_dir = config().get("log_file")
    
    if not log_dir:
        raise ValueError("❌ 'log_file' path not defined in config.")

    # Ensure directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Log filename with date-time
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = os.path.join(log_dir, f"etl_log_{timestamp}.log")

    # Create logger
    logger = logging.getLogger(name if name else __name__)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
