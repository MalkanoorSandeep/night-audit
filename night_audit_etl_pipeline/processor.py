from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import os
import pdfplumber
import fitz
import camelot
import pandas as pd
import traceback
import logging
from datetime import datetime
from sqlalchemy import create_engine
from night_audit_etl_pipeline.config_loader import config
from night_audit_etl_pipeline.logger import setup_logger
from night_audit_etl_pipeline.db_utils import *
from night_audit_etl_pipeline.email_alerts import send_email
from night_audit_etl_pipeline.helpers import convert_date, safe_float, is_strictly_numeric, extract_amount , clean_column_names, add_metadata, clean_numeric_column
from night_audit_etl_pipeline.extractors import *




logger = logging.getLogger("night_audit_etl")


def finalize_etl_run(engine, filename, section_statuses):
    loaded_rows = sum(v if isinstance(v, int) else 0 for _, v in section_statuses)
    failed_sections = [name for name, v in section_statuses if v == 'FAIL']

    status = 'PARTIAL' if failed_sections else 'SUCCESS'
    message = f"Failed sections: {', '.join(failed_sections)}" if failed_sections else None
    update_file_tracker(engine, filename, status, loaded_rows, message)

    logger.info(f"✅ Completed {filename} | Rows loaded: {loaded_rows} | Failed: {failed_sections if failed_sections else 'None'}")





def process_pdf_folder(pdf_folder_path, mysql_conn_str, logger_initializer=None):
    pdf_files = sorted(f for f in os.listdir(pdf_folder_path) if f.endswith(".pdf") and "night audit" in f.lower())
    args_list = [(pdf_folder_path, f, mysql_conn_str) for f in pdf_files] 
    num_workers = min(cpu_count(), len(pdf_files))

    logger.info(f"🚀 Starting multiprocessing with {num_workers} workers...")


    results = []

    with Pool(processes=num_workers, initializer=logger_initializer) as pool:
        for result in tqdm(pool.imap_unordered(process_pdf_task, args_list), total=len(args_list), desc="Processing PDFs"):
            if result:
                results.append(result)


    total_files = len(results)
    total_rows = sum(r.get("rows", 0) for r in results)
    skipped_files = [r["filename"] for r in results if r["status"] == "SKIPPED"]
    failed_files = [r["filename"] for r in results if r["status"] == "FAIL"]
    successful_files = [r["filename"] for r in results if r["status"] == "SUCCESS"]

    subject = "[ETL Summary] Night Audit ETL Completed"
    body = (
        f"✅ Total Files Processed: {total_files}\n"
        f"📄 Files Succeeded: {len(successful_files)}\n"
        f"⏭️ Files Skipped: {len(skipped_files)}\n"
        f"❌ Files Failed: {len(failed_files)}\n"
        f"📊 Total Rows Loaded: {total_rows}\n\n"
    )

    if skipped_files:
        body += f"\n⏭️ Skipped Files:\n" + "\n".join(skipped_files)
    if failed_files:
        body += f"\n❌ Failed Files:\n" + "\n".join(failed_files)

    send_email(subject, body)




def process_pdf_task(args):
    pdf_folder, filename, conn_str = args
    full_path = os.path.join(pdf_folder, filename)
    local_engine = create_db_engine(conn_str)  #---- added

    if is_file_already_processed(local_engine, filename):
        logger.info(f"⏭️ Skipping already processed file in worker: {filename}")
        return {"filename": filename, "status": "SKIPPED", "rows": 0}
    
    
    return process_pdf(full_path, filename, local_engine)


def handle_section(engine, section_name, extract_func, table_name, filename,  list_of_pages=None, full_text=None,  
                   prop_code=None, user_id=None, report_date=None, business_date=None,
                   clean_map=None, numeric_cols=None, postprocess=None):
    try:
        df = extract_func(list_of_pages) if list_of_pages else extract_func(full_text)
        if postprocess:
            df = postprocess(df)
        if not df.empty:
            if clean_map: df = clean_column_names(df, clean_map)
            if numeric_cols: df = clean_numeric_column(df, numeric_cols)
            df = add_metadata(df, prop_code, user_id, report_date, business_date)
            insert_dataframe(engine, df, table_name, filename)
            logger.info(f"✅ Processed {section_name}")
            return (section_name, len(df))
        else:
            logger.warning(f"⚠️ No {section_name} data in {filename}")
            return (section_name, "EMPTY")
    except Exception as e:
        logger.error(f"❌ {section_name} extraction failed: {e}\n{traceback.format_exc()}")
        return (section_name, "FAIL")
    


def handle_custom_section(engine, section_name, extract_func, filename, insert_specs, postprocess=None):
    try:
        result = extract_func()
        if not isinstance(result, tuple):
            result = (result,)  

        elif isinstance(result, tuple):
            result = list(result) 

        for df, (table_name, extras) in zip(result, insert_specs):
            if isinstance(df, pd.DataFrame) and not df.empty:
                if df.index.name or df.index.names != [None]:
                    df = df.reset_index()
                df = clean_column_names(df)
                df = df.dropna(how="all")
                if postprocess:
                    df = postprocess(df)
                if extras:
                    for col, val in extras.items():
                        df[col] = val
                insert_dataframe(engine, df, table_name, filename)
                logger.info(f"✅ Processed {section_name} → {table_name} ({len(df)} rows)")
            else:
                logger.warning(f"⚠️ No {section_name} data for {table_name} in {filename}")
    except Exception as e:
        logger.error(f"❌ {section_name} extraction failed: {e}\n{traceback.format_exc()}")





def extract_ledger_summary_wrapper(full_text_or_pages, pdf=None, business_date=None, user_id=None):
    return extract_ledger_summary_with_metadata(pdf)

def extract_no_show_wrapper(full_text_or_pages, pdf=None, business_date=None, user_id=None):
    return extract_no_show_report(full_text_or_pages["pages"], full_text_or_pages["text"], full_text_or_pages["pdf_path"])

def extract_rate_discrepancy_wrapper(full_text_or_pages, pdf=None, business_date=None, user_id=None):
    records = extract_rate_discrepancy(full_text_or_pages["page_texts"])
    if records:
        return pd.DataFrame(records, columns=[
            'start_date', 'guest_name', 'end_date', 'room', 'account', 'adults_children',
            'rate_plan', 'market', 'source', 'configured_rate', 'override_rate', 'difference'
        ])
    return pd.DataFrame()


def process_pdf(pdf_path, filename, engine):
    logger.info(f"📄 Starting processing file: {filename}")
    try:
        with pdfplumber.open(pdf_path) as pdf, fitz.open(pdf_path) as doc:
            page_texts = [page.get_text() for page in doc]
            list_of_pages = [page.extract_text().split('\n') for page in pdf.pages]
            full_text = "\n".join(["\n".join(p) for p in list_of_pages])
            camelot_tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream', strip_text='\n')
    except Exception as e:
        logger.error(f"❌ Failed to open PDF: {filename} | Error: {e}")
        update_file_tracker(filename, 'FAILURE', None, f"Open PDF error: {e}")
        return

    business_date, prop_code, user_id, report_date = extract_metadata(list_of_pages)
    section_statuses = []

    section_statuses.append(handle_section(
        engine, "A/R Aging", extract_ar_aging, "ar_aging", filename,
        list_of_pages=list_of_pages, prop_code=prop_code, user_id=user_id, report_date=report_date,
        clean_map={"30days": "days_30", "60days": "days_60", "90days": "days_90", "120days": "days_120", "limit": "limit_amount"},
        numeric_cols=['current','days_30','days_60','days_90','days_120','credits','balance','limit_amount']
    ))

    section_statuses.append(handle_section(
        engine, "Transaction Closeout", extract_transaction_closeout, "transaction_closeout", filename,
        list_of_pages=list_of_pages, prop_code=prop_code, user_id=user_id, business_date=business_date,
        clean_map={"'": ""}, numeric_cols=['opening_balance','todays_total','todays_adjustments','todays_net','ptd_totals','ytd_totals']
    ))

    section_statuses.append(handle_section(
        engine, "In-House List", extract_inhouse_df, "inhouse_list_data", filename,
        list_of_pages=list_of_pages, prop_code=prop_code, business_date=business_date
    ))

    # Hotel Statistics Sections
    for name, (start, end) in {
        "room_statistics": ("Room Statistics", "Performance Statistics"),
        "performance_statistics": ("Performance Statistics", "Revenue"),
        "guest_statistics": ("Guest Statistics", "Today's Activity")
    }.items():
        section_text = extract_section_text(full_text, start, end)
        if section_text:
            section_statuses.append(handle_section(
                engine, name, lambda x: parse_hotel_statistics(x, business_date), name, filename,
                full_text=section_text, business_date=business_date
            ))
        else:
            logger.warning(f"⚠️ Section {name} not found in {filename}")
            section_statuses.append((name, "NOT FOUND"))

    # Ledger Activity
    section_statuses.append(handle_section(
        engine, "Ledger Activity", extract_ledger_activity_report_with_metadata, "ledger_activity", filename,
        full_text=full_text
    ))

        # Ledger Summary
    section_statuses.append(handle_section(
        engine, "Ledger Summary", lambda _: extract_ledger_summary_wrapper(None, pdf), "ledger_summary", filename
    ))

    # No Show Report
    section_statuses.append(handle_section(
        engine, "No Show Report", lambda _: extract_no_show_wrapper({
            "pages": list_of_pages,
            "text": full_text,
            "pdf_path": pdf_path
        }), "no_show_report", filename
    ))

    # Rate Discrepancy
    section_statuses.append(handle_section(
        engine, "Rate Discrepancy", lambda _: extract_rate_discrepancy_wrapper({
            "page_texts": page_texts
        }), "rate_discrepancy", filename
    ))


    section_statuses.append(handle_section(
        engine, "Hotel Journal Summary",
        lambda _: extract_hotel_journal_summary(camelot_tables, filename, business_date),
        "hotel_journal_summary",
        filename
    ))

    # 12. Hotel Journal Detail
    section_statuses.append(handle_section(
    engine, "Hotel Journal Detail",
    lambda pages: extract_hotel_journal_details(pages).assign(
        date=lambda df: df["date"].apply(convert_date),
        posting_date=lambda df: df["posting_date"].apply(convert_date)
    ),
    "hotel_journal_detail",
    filename,
    list_of_pages=list_of_pages
    ))


    # ✅ Reservation Activity
    handle_custom_section(
        engine, "Reservation Activity",
        lambda: extract_reservation_activity(page_texts),
        filename,
        insert_specs=[("reservation_activity", {})],
        postprocess=lambda df: df.assign(
            arrive=df['arrive'].apply(convert_date),
            depart=df['depart'].apply(convert_date),
            reserve_date=df['reserve_date'].apply(convert_date),
            rate=df['rate'].apply(safe_float)
        )
    )

    # ✅ Shift Reconciliation
    handle_custom_section(
        engine, "Shift Reconciliation",
        lambda: extract_shift_reconciliation(pdf),
        filename,
        insert_specs=[
            ("shift_reconciliation", {}),
            ("shift_summary", {})
        ]
    )

    # ✅ Tax Exempt
    tax_dfs = extract_tax_exempt(page_texts)
    tax_business_date = tax_dfs[-1]
    handle_custom_section(
        engine, "Tax Exempt",
        lambda: tax_dfs[:4],
        filename,
        insert_specs=[
            ("exempt_revenue_tax", {"business_date": tax_business_date}),
            ("exempt_tax", {"business_date": tax_business_date}),
            ("tax_exempt_revenue_summary", {"business_date": tax_business_date}),
            ("tax_refund_revenue_summary", {"business_date": tax_business_date})
        ]
    )

    handle_custom_section(
    engine, "Gross Room Revenue",
    lambda: extract_gross_room_revenue(camelot_tables, filename, business_date),
    filename,
    insert_specs=[("gross_room_revenue_detail", {})]
    )


    handle_custom_section(
    engine, "Revenue by Rate Code",
    lambda: extract_revenue_by_rate_code(camelot_tables, filename),
    filename,
    insert_specs=[("revenue_by_rate_code", {})]
    )


    handle_custom_section(
    engine, "Advance Deposit Journal",
    lambda: extract_advance_deposit_journal(list_of_pages),
    filename,
    insert_specs=[("advance_deposit_journal", {"business_date": pd.to_datetime(business_date).date() if business_date else None})]
    )
        # --- Final summary ---
    finalize_etl_run(engine,filename, section_statuses)

    status = "FAIL" if any(status == "FAIL" for _, status in section_statuses) else "SUCCESS"
    loaded_rows = sum(v if isinstance(v, int) else 0 for _, v in section_statuses)

    return {
        "filename": filename,
        "status": status,
        "rows": loaded_rows
    }