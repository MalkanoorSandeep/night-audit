import os
from datetime import datetime
from night_audit_etl_pipeline.logger import setup_logger
from multiprocessing import freeze_support
from night_audit_etl_pipeline.logger import setup_logger
from night_audit_etl_pipeline.config_loader import config
from night_audit_etl_pipeline.processor import process_pdf_folder
from night_audit_etl_pipeline.db_utils import *
from night_audit_etl_pipeline.helpers import *

    # 🔧 Define multiprocessing logger initializer
def init_worker_logger():
    setup_logger("night_audit_etl")


if __name__ == "__main__":
    
    freeze_support()

    # 🔧 Set log path for this run
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = f"./logs/night_audit_log_{timestamp}.log"
    os.environ["LOG_FILE_PATH"] = log_path

    # 🔧 Setup main logger
    logger = setup_logger("night_audit_etl")
    logger.info("🚀 ETL started")


    config_dict = config()
    pdf_folder = config_dict.get("pdf_folder")
    mysql_conn_str = config_dict.get("mysql_conn")

    if not pdf_folder or not mysql_conn_str:
        logger.error("❌ Missing PDF folder path or MySQL connection string")
    else:
        process_pdf_folder(pdf_folder, mysql_conn_str, init_worker_logger)
