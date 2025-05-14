# db_utils.py

from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import time
import traceback
import logging

logger = logging.getLogger(__name__)

# def create_db_engine(mysql_conn_str):
#     return create_engine(mysql_conn_str)

def create_db_engine(mysql_conn_str):
    engine = create_engine(mysql_conn_str)
    logger.info(f"🔍 Engine type: {type(engine)}")
    return engine

def update_file_tracker(engine, filename, status, row_count=None, error_message=None):
    sql = text("""
        INSERT INTO file_tracker (source_file, load_date, status, rows_loaded, error_message)
        VALUES (:filename, NOW(), :status, :row_count, :error_message)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "filename": filename,
            "status": status,
            "row_count": row_count,
            "error_message": error_message
        })


def is_file_already_processed(engine, filename):
    try:
        sql = text("""
            SELECT COUNT(*) FROM file_tracker
            WHERE source_file = :filename AND status IN ('SUCCESS', 'PARTIAL')
        """)
        with engine.connect() as conn:
            result = conn.execute(sql, {"filename": filename})
            return result.scalar() > 0
    except Exception as e:
        logger.warning(f"⚠️ Failed to check file_tracker for {filename}: {e}")
        return False


def insert_dataframe(engine, df, table_name, filename, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            df["source_file"] = filename
            df["load_timestamp"] = datetime.now()
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            logger.info(f"✅ Loaded {len(df)} rows into {table_name} from {filename}")
            return
        except Exception as e:
            logger.error(f"❌ Insert attempt {attempt + 1} failed for {table_name}: {e}\n{traceback.format_exc()}")
            attempt += 1
            if attempt < retries:
                logger.info(f"🔁 Retrying insert into {table_name} after {delay} seconds...")
                time.sleep(delay)
            else:
                raise





