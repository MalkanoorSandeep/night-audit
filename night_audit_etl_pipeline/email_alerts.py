import smtplib
from email.mime.text import MIMEText
from night_audit_etl_pipeline.logger import setup_logger
from night_audit_etl_pipeline.config_loader import config as load_config
from datetime import datetime

logger = setup_logger(__name__)

def send_email(subject, message):
    try:
        email_cfg = load_config()['email']
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = email_cfg['sender']
        msg['To'] = email_cfg['receiver']
        with smtplib.SMTP(email_cfg['smtp_server'], email_cfg['smtp_port']) as server:
            server.starttls()
            server.login(email_cfg['username'], email_cfg['password'])
            server.send_message(msg)
        logger.info("📧 Email sent successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")


def notify_result(filename, loaded_rows, failed_sections):
    if failed_sections:
        subject = f"[ETL Result] {filename} processed @ {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg = f"{filename} was processed with partial success.\n\nRows loaded: {loaded_rows}\nFailed sections: {', '.join(failed_sections)}"
    else:
        subject = f"[ETL Result] {filename} processed"
        msg = f"{filename} was processed successfully.\n\nTotal rows loaded: {loaded_rows}"
    send_email(subject, msg)