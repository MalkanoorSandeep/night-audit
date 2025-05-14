from multiprocessing import freeze_support
from night_audit_etl_pipeline.config_loader import config
from night_audit_etl_pipeline.logger import setup_logger
from night_audit_etl_pipeline.processor import process_pdf_folder
from night_audit_etl_pipeline.db_utils import *
from night_audit_etl_pipeline.helpers import *

# Initialize logger
logger = setup_logger(__name__)

if __name__ == "__main__":
    freeze_support()
    config_dict = config()
    pdf_folder = config_dict.get("pdf_folder")
    mysql_conn_str = config_dict.get("mysql_conn")

    if not pdf_folder or not mysql_conn_str:
        logger.error("❌ Missing PDF folder path or MySQL connection string in config.")
    else:
        logger.info("🚀 Starting ETL pipeline...")
        process_pdf_folder(pdf_folder, mysql_conn_str