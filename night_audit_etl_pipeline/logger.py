# night_audit_etl_pipeline/logger.py

import logging
import os
from datetime import datetime

def setup_logger(name="night_audit_etl", log_level="INFO"):
    log_file_path = os.getenv("LOG_FILE_PATH")

    if not log_file_path:
        raise ValueError("❌ 'LOG_FILE_PATH' environment variable not set.")

    log_dir = os.path.dirname(log_file_path)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
