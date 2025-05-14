import re
import pandas as pd
import camelot
import fitz
import pdfplumber
from datetime import datetime
import traceback
from night_audit_etl_pipeline.helpers import convert_date, safe_float, is_strictly_numeric, extract_amount , clean_column_names, add_metadata, clean_numeric_column
from night_audit_etl_pipeline.logger import setup_logger


logger = setup_logger(__name__)

def extract_metadata(list_of_pages):
    business_date = prop_code = user_id = report_date = None
    for lines in list_of_pages:
        for line in lines:
            if "Business Date:" in line:
                match = re.search(r'Business Date:\s*(\d{1,2}/\d{1,2}/\d{4})', line)
                if match: business_date = match.group(1)
            if "Property Code:" in line:
                match = re.search(r'Property Code:\s*(\S+)', line)
                if match: prop_code = match.group(1)
            if "User:" in line:
                match = re.search(r'User:\s*(\S+)', line)
                if match: user_id = match.group(1)
            if "Date/Time of Printing:" in line:
                match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', line)
                if match: report_date = match.group(1)
    return business_date, prop_code, user_id, report_date

# === Insert all utility and extractor functions here (unchanged) ===

def extract_ar_aging(list_of_pages):
    rows = []
    header = ['Account', 'Guest Name', 'Current', '30Days', '60Days', '90Days', '120Days', 'Credits', 'Balance', 'Limit']
    collecting = False
    for lines in list_of_pages:
        for line in lines:
            if "A/R Aging" in line:
                collecting = True
                continue
            if collecting and any(stop in line for stop in ["Grand Total", "Advance Deposit", "Transaction Code", "Ledger", "Date/Time"]):
                collecting = False
                continue
            if collecting and ("Account Name" in line or not line.strip()):
                continue
            tokens = line.strip().split()
            if len(tokens) >= 10:
                account = tokens[0]
                numeric_tail = tokens[-8:]
                guest_name = " ".join(tokens[1:-8])
                if re.fullmatch(r"\d+", account) and all(is_strictly_numeric(val) for val in numeric_tail):
                    rows.append([account, guest_name] + numeric_tail)
    return pd.DataFrame(rows, columns=header) if rows else pd.DataFrame(columns=header)

def extract_transaction_closeout(list_of_pages):
    pattern = re.compile(r'^(.*?)\s+([-0-9,().]+)\s+([-0-9,().]+)\s+([-0-9,().]+)\s+([-0-9,().]+)\s+([-0-9,().]+)\s+([-0-9,().]+)$')
    results = []
    for lines in list_of_pages:
        collecting = False
        for line in lines:
            if "Final Transaction Closeout" in line:
                collecting = True; continue
            if collecting and any(k in line for k in ["Date/Time of Printing", "Gross Room Revenue", "Totals:"]):
                collecting = False; continue
            if collecting:
                match = pattern.match(line.strip())
                if match:
                    results.append(match.groups())
    cols = ["Description", "Opening Balance", "Today's Total", "Today's Adjustments", "Today's Net", "PTD Totals", "YTD Totals"]
    return pd.DataFrame(results, columns=cols) if results else pd.DataFrame(columns=cols)

# def extract_inhouse_lines(list_of_pages):
#     lines = []
#     for page_lines in list_of_pages:
#         if any("In House List" in line for line in page_lines):
#             lines.extend(line.strip() for line in page_lines if line.strip())
#     return lines

# def parse_inhouse_list_with_confirmation(lines):
#     records = []
#     for line in lines:
#         parts = line.split()
#         if len(parts) >= 9 and re.match(r'^\d{3}$', parts[0]) and parts[1].isdigit():
#             room, account = parts[0], parts[1]
#             guest_parts, i = [], 2
#             while i < len(parts) and not re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', parts[i]): guest_parts.append(parts[i]); i += 1
#             guest_name_raw = " ".join(guest_parts)
#             conf_match = re.search(r'(\d{6,8})$', guest_name_raw)
#             confirmation_number = conf_match.group(1) if conf_match else ""
#             guest_name = guest_name_raw[:conf_match.start()].strip() if conf_match else guest_name_raw.strip()
#             records.append({
#                 "room": room, "account": account, "guest_name": guest_name, "confirmation_notes": confirmation_number,
#                 "arrive": "", "depart": "", "ppl": "", "type": "", "rate_code": "", "rate": "",
#                 "gtd": "", "source": "", "market": "", "balance": ""
#             })
#     return pd.DataFrame(records)

# def extract_inhouse_df(list_of_pages):
#     inhouse_lines = extract_inhouse_lines(list_of_pages)
#     df = parse_inhouse_list_with_confirmation(inhouse_lines)
#     if df.empty:
#         return df
#     df["arrive"] = df["arrive"].apply(convert_date)
#     df["depart"] = df["depart"].apply(convert_date)
#     df["rate"] = df["rate"].apply(safe_float)
#     df["balance"] = df["balance"].apply(safe_float)
#     return df


# import pandas as pd
# import re
# from datetime import datetime

# --- Helper Functions ---
def extract_inhouse_lines(list_of_pages):
    lines = []
    for page_lines in list_of_pages:
        if any("In House List" in line for line in page_lines):
            lines.extend(line.strip() for line in page_lines if line.strip())
    return lines

def parse_inhouse_list_with_confirmation(lines):
    records = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 9 and re.match(r'^\d{3}$', parts[0]) and parts[1].isdigit():
            room = parts[0]
            account = parts[1]
            guest_parts, idx = [], 2
            while idx < len(parts) and not re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', parts[idx]):
                guest_parts.append(parts[idx])
                idx += 1
            guest_name_raw = " ".join(guest_parts)
            confirmation_number = ""
            match = re.search(r'(\d{6,8})$', guest_name_raw)
            if match:
                confirmation_number = match.group(1)
                guest_name = guest_name_raw[:match.start()].strip()
            else:
                guest_name = guest_name_raw.strip()
            
            arrive = depart = ppl = rtype = rate_code = rate = gtd = source = market = balance = ""
            if idx + 7 <= len(parts):
                arrive = parts[idx]
                depart = parts[idx+1]
                ppl = parts[idx+2]
                rtype = parts[idx+3]
                rate_code = parts[idx+4]
                rate = parts[idx+5]
                last_fields = parts[-4:]
                balance = last_fields[-1] if last_fields else ""
                market = last_fields[-2] if len(last_fields) >= 2 else ""
                source = last_fields[-3] if len(last_fields) >= 3 else ""
                gtd = last_fields[0] if len(last_fields) == 4 and len(last_fields[0]) <= 4 else ""

            records.append({
                "room": room, "account": account, "guest_name": guest_name, "confirmation_notes": confirmation_number,
                "arrive": arrive, "depart": depart, "ppl": ppl, "type": rtype, "rate_code": rate_code,
                "rate": rate, "gtd": gtd, "source": source, "market": market, "balance": balance
            })
    return pd.DataFrame(records)

def extract_inhouse_df(list_of_pages):
    inhouse_lines = extract_inhouse_lines(list_of_pages)
    df = parse_inhouse_list_with_confirmation(inhouse_lines)
    if df.empty:
        return df
    df["arrive"] = df["arrive"].apply(convert_date)
    df["depart"] = df["depart"].apply(convert_date)
    df["rate"] = df["rate"].apply(safe_float)
    df["balance"] = df["balance"].apply(safe_float)
    return df


def extract_section_text(text, start_marker, end_marker=None):
    start_idx = text.find(start_marker)
    if start_idx == -1:
        return None
    if end_marker:
        end_idx = text.find(end_marker, start_idx)
        return text[start_idx + len(start_marker):end_idx].strip() if end_idx != -1 else text[start_idx + len(start_marker):].strip()
    return text[start_idx + len(start_marker):].strip()


def parse_hotel_statistics(section_text, business_date):
    lines = section_text.splitlines()
    records = []
    for idx, line in enumerate(lines):
        tokens = re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?\s?%?|[^\d\s][^0-9]*', line.strip())
        tokens = [t.strip() for t in tokens if t.strip()]
        if len(tokens) >= 5:
            metric = " ".join(tokens[:-5])
            values = tokens[-5:]
            records.append([metric] + values)
        else:
            logger.debug(f"⚠️ Skipped malformed Hotel Statistics line {idx+1}: {line}")
    headers = ["metric", "today", "current_ptd", "last_year_ptd", "current_ytd", "last_ytd"]
    df = pd.DataFrame(records, columns=headers) if records else pd.DataFrame(columns=headers)
    if not df.empty and len(df) > 1: df = df.iloc[1:].reset_index(drop=True)
    try:
        formatted_date = datetime.strptime(business_date, "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        formatted_date = None
    df["business_date"] = formatted_date
    return df



def extract_ledger_activity_report_with_metadata(full_text):
    start_idx = full_text.find('Ledger Activity Report')
    end_idx = full_text.find('Ledger Summary')
    if start_idx == -1:
        raise Exception("Ledger Activity Report section not found")
    ledger_text = full_text[start_idx:end_idx] if end_idx != -1 else full_text[start_idx:]
    lines = ledger_text.splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    business_date = user = None
    for line in lines:
        if "Business Date:" in line:
            match = re.search(r'Business Date:\s*([\d/]+)', line)
            if match: business_date = match.group(1)
        if "User:" in line:
            match = re.search(r'User:\s*(\S+)', line)
            if match: user = match.group(1)
    ledgers = []
    current_entry = {}
    current_ledger = None
    for line in lines:
        if line in ['Guest', 'Accounts Receivable', 'Advance Deposit']:
            if current_entry:
                ledgers.append(current_entry)
            current_ledger = line
            current_entry = {'ledger_type': current_ledger}
        elif 'Opening Balance' in line:
            current_entry['opening_balance'] = extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Credits' in line:
            current_entry['credits'] = extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Adjustments' in line:
            current_entry['adjustments'] = extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Debits' in line:
            current_entry['debits'] = extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Transfer' in line:
            if 'transfers' not in current_entry:
                current_entry['transfers'] = 0
            current_entry['transfers'] += extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Balance Forward' in line and 'Total Balance Forward' not in line:
            current_entry['balance_forward'] = extract_amount(re.search(r'-?\(?[\d,.]+\)?', line).group(0))
        elif 'Total Balance Forward' in line:
            break
    if current_entry:
        ledgers.append(current_entry)
    df = pd.DataFrame(ledgers)
    df['business_date'] = pd.to_datetime(business_date, format='%m/%d/%Y') if business_date else None
    df['user_id'] = user
    return df


def extract_ledger_summary_with_metadata(pdf):
    full_text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    start_idx = full_text.find("Ledger Summary")
    if start_idx == -1:
        logger.warning("⚠️ Ledger Summary section not found.")
        return pd.DataFrame()

    ledger_text = full_text[start_idx:]
    lines = [line.strip() for line in ledger_text.splitlines() if line.strip()]

    # Metadata
    business_date = None
    user_id = None

    if "Business Date:" in full_text:
        date_match = re.search(r'Business Date:\s*([\d/]+)', full_text)
        if date_match:
            business_date = date_match.group(1)

    if "User:" in full_text:
        user_match = re.search(r'User:\s*(\S+)', full_text)
        if user_match:
            user_id = user_match.group(1)

    def extract_amount(text):
        match = re.search(r'-?\(?[\d,.]+\)?', text)
        if match:
            num = match.group(0).replace(',', '')
            return -float(num.replace('(', '').replace(')', '')) if '(' in num and ')' in num else float(num)
        return None

    records = []
    current_section = None

    for line in lines:
        if "Guest Ledger Summary" in line:
            current_section = "Guest Ledger"
        elif "Accounts Receivable Ledger Summary" in line:
            current_section = "Accounts Receivable Ledger"
        elif "Advance Deposit Summary" in line:
            current_section = "Advance Deposit Ledger"
        elif "Total Balance" in line:
            current_section = "Total Ledger"
        elif any(keyword in line for keyword in ["Subtotal", "Closing Balance", "Opening Balance", "Balance Forward", "Net Change"]):
            field = re.sub(r':.*', '', line).strip()
            amount = extract_amount(line)
            if amount is not None:
                records.append({
                    "section": current_section,
                    "field_name": field,
                    "amount": amount,
                    "business_date": pd.to_datetime(business_date, format='%m/%d/%Y') if business_date else None,
                    "user_id": user_id
                })

    df = pd.DataFrame(records)
    return df



def extract_no_show_report(list_of_pages, full_text, pdf_path):
    start_idx = full_text.find("No Show Report")
    if start_idx == -1:
        logger.warning("⚠️ No Show Report section not found in file:")
        return pd.DataFrame()

    no_show_text = full_text[start_idx:]
    lines = [line.strip() for line in no_show_text.splitlines() if line.strip()]

    business_date = user_id = None
    if "Business Date:" in full_text:
        date_match = re.search(r'Business Date:\s*([\d/]+)', full_text)
        if date_match:
            business_date = date_match.group(1)
    if "User:" in full_text:
        user_match = re.search(r'User:\s*(\S+)', full_text)
        if user_match:
            user_id = user_match.group(1)

    records, headers_found = [], False
    for line in lines:
        if "Account" in line and "Guest Name" in line:
            headers_found = True
            continue
        if headers_found:
            if line.startswith("Total No Shows") or line.startswith("Total No-Show"):
                break
            account_match = re.match(r'^(\d+)', line)
            if account_match:
                account = account_match.group(1)
                line_rest = line[len(account):].strip()
                name_match = re.match(r'(.+?)\s+(\d{1,2}/\d{1,2}/\d{2})\s+(\d{1,2}/\d{1,2}/\d{2})\s+(.*)', line_rest)
                if name_match:
                    guest_name = name_match.group(1).strip()
                    arrival = name_match.group(2).strip()
                    departure = name_match.group(3).strip()
                    rest_fields = name_match.group(4).strip()
                    rest_parts = re.split(r'\s+', rest_fields)
                    source = rest_parts[0] if len(rest_parts) > 0 else None
                    gtd = rest_parts[1] if len(rest_parts) > 1 else None
                    rate_plan = rest_parts[2] if len(rest_parts) > 2 else None
                    rate = float(rest_parts[3].replace(',', '')) if len(rest_parts) > 3 else None
                    balance = float(rest_parts[4].replace(',', '')) if len(rest_parts) > 4 else None
                    payment = float(rest_parts[5].replace(',', '')) if len(rest_parts) > 5 else None
                    auth_status = rest_parts[6] if len(rest_parts) > 6 else None
                    records.append({
                        "account": account, "guest_name": guest_name,
                        "arrival_date": pd.to_datetime(arrival, format='%m/%d/%y') if arrival else None,
                        "departure_date": pd.to_datetime(departure, format='%m/%d/%y') if departure else None,
                        "source": source, "gtd": gtd, "rate_plan": rate_plan, "rate": rate,
                        "balance": balance, "payment": payment, "auth_status": auth_status,
                        "business_date": pd.to_datetime(business_date, format='%m/%d/%Y') if business_date else None,
                        "user_id": user_id
                    })
    return pd.DataFrame(records)



def extract_rate_discrepancy(page_texts):
    capture = False
    text_blocks = []
    for text in page_texts:
        if "Rate Discrepancy Report" in text:
            capture = True
        if capture:
            if "Reservation Activity Report" in text:
                text = text.split("Reservation Activity Report")[0]
            text_blocks.append(text)
    full_text = "\n".join(text_blocks)
    lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    records = []
    i = 0
    date_pattern = r"\d{1,2}/\d{1,2}/\d{4}"
    adult_child_pattern = r"\d+\s*/\s*\d+"
    source_options = ['CRS', 'DIRECT']
    while i < len(lines):
        line = lines[i]
        if line.isdigit() and len(line) == 3 and (i+1) < len(lines) and lines[i+1].isdigit() and len(lines[i+1]) == 9:
            room = line
            account = lines[i+1]
            guest_block = []
            j = i + 2
            while j < len(lines):
                if lines[j].isdigit() and len(lines[j]) == 3 and (j+1) < len(lines) and lines[j+1].isdigit() and len(lines[j+1]) == 9:
                    break
                guest_block.append(lines[j])
                j += 1
            guest_text = " ".join(guest_block)
            guest_name = ""
            adults_children = ""
            start_date = ""
            rate_plan = ""
            market = ""
            source = ""
            configured_rate = ""
            override_rate = ""
            difference = ""
            end_date = ""
            ac_match = re.search(adult_child_pattern, guest_text)
            if ac_match:
                adults_children = ac_match.group()
                guest_name = guest_text[:ac_match.start()].strip()
                after_ac = guest_text[ac_match.end():]
                date_match = re.search(date_pattern, after_ac)
                if date_match:
                    start_date = date_match.group()
                    after_date = after_ac[date_match.end():].strip()
                    after_parts = after_date.split()
                    if after_parts:
                        rate_plan = after_parts[0]
                        after_parts = after_parts[1:]
                    market_parts = []
                    while after_parts and after_parts[0] not in source_options:
                        market_parts.append(after_parts[0])
                        after_parts = after_parts[1:]
                    market = " ".join(market_parts)
                    if after_parts:
                        source = after_parts[0]
                        after_parts = after_parts[1:]
                    if len(after_parts) >= 3:
                        configured_rate = after_parts[0]
                        override_rate = after_parts[1]
                        difference = after_parts[2]
                        after_parts = after_parts[3:]
                    for part in after_parts:
                        if re.match(date_pattern, part):
                            end_date = part
                            break
            if guest_name and adults_children and start_date:
                converted_start_date = convert_date(start_date)
                converted_end_date = convert_date(end_date) if end_date else converted_start_date
                record = (
                    converted_start_date,
                    guest_name,
                    converted_end_date,
                    room,
                    account,
                    adults_children,
                    rate_plan,
                    market,
                    source,
                    safe_float(configured_rate),
                    safe_float(override_rate),
                    safe_float(difference)
                )
                records.append(record)
            i = j
        else:
            i += 1
    return records



# def extract_hotel_journal_details(list_of_pages):
#     entries, current_code = [], None
#     skip_keywords = ["Posting Date", "Account Type", "Total For", "Grand Total", "Subtotal", "Date Range", "Software Version"]
#     for page_idx, lines in enumerate(list_of_pages):
#         collecting = False
#         for line_idx, line in enumerate(lines):
#             line = line.strip()
#             if "Hotel Journal Detail" in line: collecting = True; continue
#             if collecting and any(k in line for k in ["Hotel Journal Summary", "Date/Time of Printing", "Totals:", "Software Version"]): collecting = False; continue
#             if line.startswith("Transaction Code:"):
#                 current_code = line.replace("Transaction Code:", "").strip(); continue
#             if not collecting or not current_code: continue
#             if any(skip in line for skip in skip_keywords): continue
#             amount_match = re.search(r'(\(?-?\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)\s+0\.00$', line)
#             if not amount_match: continue
#             try:
#                 amount_val = amount_match.group(1).replace('$','').replace(',','').replace('(','-').replace(')','')
#                 amount = float(amount_val)
#                 line_cleaned = line.replace(amount_match.group(0), '').strip()
#                 tokens = line_cleaned.split()
#                 if len(tokens) < 8: continue
#                 date, posting_date, time, am_pm, user_id = tokens[:5]
#                 shift_id = tokens[5] if tokens[5].isdigit() else None
#                 room = tokens[6] if shift_id and len(tokens) > 6 and tokens[6].isdigit() else None
#                 acc_idx = 7 if shift_id else 6
#                 account_type = tokens[acc_idx]
#                 account_number = tokens[acc_idx+1] if len(tokens) > acc_idx+1 and tokens[acc_idx+1].isdigit() else None
#                 guest_name = " ".join(tokens[acc_idx+2:]) if account_number else " ".join(tokens[acc_idx+1:])
#                 entries.append({
#                     "transaction_code": current_code, "date": date, "posting_date": posting_date, "time": time,
#                     "am_pm": am_pm, "user_id": user_id, "shift_id": shift_id, "room": room, "account_type": account_type,
#                     "account_number": account_number, "guest_name": guest_name.strip(), "amount": amount
#                 })
#             except Exception as e:
#                 logger.debug(f"⚠️ Failed parsing journal at page {page_idx+1} line {line_idx+1}: {e}")
#     return pd.DataFrame(entries)


def extract_hotel_journal_details(list_of_pages):
    entries, current_code = [], None
    skip_keywords = ["Posting Date", "Account Type", "Total For", "Grand Total", "Subtotal", "Date Range", "Software Version"]

    for page_idx, lines in enumerate(list_of_pages):
        collecting = False
        for line_idx, line in enumerate(lines):
            line = line.strip()
            if "Hotel Journal Detail" in line:
                collecting = True
                continue
            if collecting and any(k in line for k in ["Hotel Journal Summary", "Date/Time of Printing", "Totals:", "Software Version"]):
                collecting = False
                continue
            if line.startswith("Transaction Code:"):
                current_code = line.replace("Transaction Code:", "").strip()
                continue
            if not collecting or not current_code:
                continue
            if any(skip in line for skip in skip_keywords):
                continue

            amount_match = re.search(r'(\(?-?\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)\s+0\.00$', line)
            if not amount_match:
                continue
            try:
                amount_val = amount_match.group(1).replace('$','').replace(',','').replace('(','-').replace(')','')
                amount = float(amount_val)
                line_cleaned = line.replace(amount_match.group(0), '').strip()
                tokens = line_cleaned.split()
                if len(tokens) < 8:
                    continue

                date = tokens[0]
                posting_date = tokens[1]
                time = tokens[2]
                am_pm = tokens[3]
                user_id = tokens[4]

                shift_candidate = tokens[5]
                shift_id = shift_candidate if shift_candidate.isdigit() and len(shift_candidate) <= 2 else None
                room_candidate = tokens[6] if shift_id else tokens[5]
                room = room_candidate if room_candidate.isdigit() else None

                acc_idx = 7 if shift_id else 6

                if len(tokens) > acc_idx + 1 and \
                    " ".join(tokens[acc_idx:acc_idx + 2]).lower() in ["guest account", "directbill account"]:
                    account_type = " ".join(tokens[acc_idx:acc_idx + 2])
                    account_number = None
                    guest_name = " ".join(tokens[acc_idx + 2:])
                else:
                    account_type = tokens[acc_idx]
                    if len(tokens) > acc_idx + 1 and tokens[acc_idx + 1].isdigit():
                        account_number = tokens[acc_idx + 1]
                        guest_name = " ".join(tokens[acc_idx + 2:])
                    else:
                        account_number = None
                        guest_name = " ".join(tokens[acc_idx + 1:])

                entries.append({
                    "transaction_code": current_code,
                    "date": date,
                    "posting_date": posting_date,
                    "time": time,
                    "am_pm": am_pm,
                    "user_id": user_id,
                    "shift_id": shift_id,
                    "room": room,
                    "account_type": account_type,
                    "account_number": account_number,
                    "guest_name": guest_name.replace("Guest Account", "").strip(),
                    "amount": amount
                })
            except Exception as e:
                logger.debug(f"⚠️ Failed parsing journal at page {page_idx+1} line {line_idx+1}: {e}")
    return pd.DataFrame(entries)



def extract_hotel_journal_summary(camelot_tables, filename, business_date):
    try:
        for idx, table in enumerate(camelot_tables):
            df = table.df
            flat_text = " ".join(df.astype(str).values.flatten())
            if "Hotel Journal Summary" in flat_text:
                keywords = ['Cash (CA)', 'Direct Bill (DB)', 'Room Charge (RM)', 'Visa Payment (VI)', 'Master Card (MC)']
                if sum(1 for k in keywords if k in flat_text) >= 3:
                    df = df.drop(index=[0, 1, 2], errors='ignore').reset_index(drop=True)
                    expected_cols = ["description", "postings", "corrections", "adjustments",
                                     "totals", "transactions", "post_count", "corr_count", "adj_count"]
                    if len(df.columns) <= len(expected_cols):
                        df.columns = expected_cols[:len(df.columns)]

                    # ✅ ADD EXTRA COLUMNS
                    df["source_file"] = filename
                    df["business_date"] = pd.to_datetime(business_date).date() if business_date else None
                    df["load_timestamp"] = datetime.now()

                    # ✅ CLEAN NUMERIC COLUMNS
                    numeric_cols = ["postings", "corrections", "adjustments", "totals",
                                    "transactions", "post_count", "corr_count", "adj_count"]
                    for col in numeric_cols:
                        df[col] = df[col].replace(['', 'NA', 'nan'], None)
                        df[col] = df[col].apply(lambda x: safe_float(x) if pd.notnull(x) else None)

                    return df
    except Exception as e:
        logger.error(f"❌ Hotel Journal Summary extraction failed: {e}\n{traceback.format_exc()}")
    return pd.DataFrame()



def extract_reservation_activity(page_texts):
    capture = False
    lines_collected = []
    start_marker = "Reservation Activity Report"
    end_marker = "Total Reservations:"
    account_regex = r'^\d{9}$'
    date_regex = r'^\d{1,2}/\d{1,2}/\d{2,4}$'

    for text in page_texts:
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if start_marker in line:
                capture = True
                continue
            if end_marker in line:
                capture = False
                break
            if capture:
                if not line or "Account Guest Name" in line or "GTD Reserve Date" in line or "Business Date" in line or "Total Reservations" in line or "Total Room Nights" in line:
                    continue
                lines_collected.append(line)
    records = []
    current_record = []
    for line in lines_collected:
        if re.match(account_regex, line):
            if current_record:
                records.append(current_record)
            current_record = [line]
        else:
            current_record.append(line)
    if current_record:
        records.append(current_record)
    final_data = []
    for record in records:
        flat = ' '.join(record).split()
        if 'SRD' in flat and 'RATE' in flat:
            srd_index = flat.index('SRD')
            if srd_index+1 < len(flat) and flat[srd_index+1] in ['RATE', 'Rate']:
                flat[srd_index] = 'SRD RATE'
                del flat[srd_index+1]
        if len(flat) >= 8:
            account = flat[0]
            guest_name_parts = []
            idx = 1
            while idx < len(flat) and not re.match(date_regex, flat[idx]):
                guest_name_parts.append(flat[idx])
                idx += 1
            guest_name = ' '.join(guest_name_parts)
            if idx >= len(flat):
                continue
            arrive = flat[idx]
            depart = flat[idx+1] if idx+1 < len(flat) else None
            nights = flat[idx+2] if idx+2 < len(flat) else None
            status = flat[idx+3] if idx+3 < len(flat) else None
            rate = flat[idx+4] if idx+4 < len(flat) else None
            rate_code = flat[idx+5] if idx+5 < len(flat) else None
            type_ = flat[idx+6] if idx+6 < len(flat) else None
            room = flat[idx+7] if idx+7 < len(flat) else None
            if room and not room.isdigit():
                source = room
                room = None
                after_source = flat[idx+8:]
            else:
                source = flat[idx+8] if idx+8 < len(flat) else None
                after_source = flat[idx+9:] if idx+9 < len(flat) else []
            if after_source:
                first = after_source[0]
                if first.isdigit():
                    crs_conf_no = first
                    gtd = after_source[1] if len(after_source) > 1 else None
                    reserve_date = after_source[2] if len(after_source) > 2 else None
                    user = ' '.join(after_source[3:]) if len(after_source) > 3 else None
                else:
                    crs_conf_no = None
                    gtd = first
                    reserve_date = after_source[1] if len(after_source) > 1 else None
                    user = ' '.join(after_source[2:]) if len(after_source) > 2 else None
            else:
                crs_conf_no = None
                gtd = None
                reserve_date = None
                user = None
            final_data.append({
                'account': account,
                'guest_name': guest_name,
                'arrive': arrive,
                'depart': depart,
                'nights': nights,
                'status': status,
                'rate': rate,
                'rate_code': rate_code,
                'type': type_,
                'room': room,
                'source': source,
                'crs_conf_no': crs_conf_no,
                'gtd': gtd,
                'reserve_date': reserve_date,
                'user': user
            })
    df = pd.DataFrame(final_data)
    return df



def extract_shift_reconciliation(pdf):
    shift_data = []
    guest_cash_data = []
    business_date = None
    found_shift = False
    found_guest = False

    for page in pdf.pages:
        text = page.extract_text()
        if not text: continue
        lines = text.split('\n')

        for line in lines:
            line_clean = line.strip()

            if "Business Date:" in line_clean and business_date is None:
                parts = line_clean.split("Business Date:")
                if len(parts) > 1:
                    raw_date = parts[1].split()[0].strip()
                    business_date = convert_date(raw_date)

            if "Shift Reconciliation Closeout" in line_clean:
                found_shift = True
                continue

            if found_shift:
                if "Grand Total" in line_clean:
                    found_shift = False
                    continue
                if "Cash (CA)" in line_clean and '(' in line_clean:
                    parts = line_clean.split()
                    if len(parts) >= 3 and parts[0].isdigit():
                        shift_id = parts[0]
                        description = parts[1] + " " + parts[2]
                        total = safe_float(parts[-1])
                        shift_data.append([business_date, shift_id, description, total])

            if "Summary by User Id / Shift Id" in line_clean:
                found_guest = True
                continue

            if found_guest:
                if "Date/Time of Printing" in line_clean:
                    found_guest = False
                    continue
                if line_clean and line_clean[0].isdigit():
                    parts = line_clean.split()
                    if len(parts) >= 6:
                        guest_cash_data.append([business_date] + parts[:6])

    shift_df = pd.DataFrame(shift_data, columns=['business_date', 'shift_id', 'description', 'total']) if shift_data else pd.DataFrame()
    shift_cash_df = pd.DataFrame(guest_cash_data, columns=['business_date', 'shift_id', 'user_id', 'beginning_bank', 'closing_bank', 'over_short', 'auto_close']) if guest_cash_data else pd.DataFrame()
    return shift_df, shift_cash_df



def extract_tax_exempt(page_texts):
    lines = []
    for page_text in page_texts:
        lines.extend(page_text.splitlines())

    tax_by_tax_rows = []
    exempt_tax_rows = []
    txn_rows = []
    refund_rows = []

    business_date = None
    current_section = None
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Extract Business Date
        if not business_date:
            match = re.search(r'Business Date:\s*(\d+/\d+/\d+)', line)
            if match:
                business_date = datetime.strptime(match.group(1), "%m/%d/%Y").date()

        # Detect Sections
        if "Tax Exempt Revenue Summary - By Tax" in line:
            current_section = "by_tax"
        elif "Exempt -" in line and "through" in line:
            current_section = "exempt_tax"
        elif "Tax Exempt Revenue Summary - By Transaction Code" in line:
            current_section = "txn"
        elif "Tax Refund Revenue Summary - By Transaction Code" in line:
            current_section = "refund"

        # Current Tax Configuration
        if "Current Tax Configuration" in line and current_section == "by_tax":
            t1_rate = t5_rate = None
            if i + 1 < len(lines):
                match1 = re.search(r"([\d.]+)%", lines[i + 1])
                if match1:
                    t1_rate = float(match1.group(1))
            if i + 2 < len(lines):
                match2 = re.search(r"([\d.]+)%", lines[i + 2])
                if match2:
                    t5_rate = float(match2.group(1))
            if t1_rate is not None and t5_rate is not None:
                tax_by_tax_rows.append({
                    "Label": "Current Tax Configuration",
                    "T1": t1_rate,
                    "T5": t5_rate
                })

        # Skip date headings
        if re.search(r'Revenue -\d+/\d+/\d+ through \d+/\d+/\d+', line):
            i += 1
            continue

        # Capture known labels
        if any(label in line for label in [
            "Exempt Revenue -PTD", "Exempt Revenue -YTD",
            "Exempt -PTD", "Exempt -YTD",
            "Refund Revenue -PTD", "Refund Revenue -YTD"
        ]):
            label = line

            if current_section == "refund":
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    nums = re.findall(r"[\d,.]+", next_line)

                    if len(nums) == 2:
                        val1 = float(nums[0].replace(",", ""))
                        val2 = float(nums[1].replace(",", ""))
                        if val1 <= 1.0 and val2 <= 1.0:
                            val1, val2 = None, None
                        refund_rows.append({"Label": label, "RM": val1, "Total Refund Revenue": val2})
                    else:
                        refund_rows.append({"Label": label, "RM": None, "Total Refund Revenue": None})
                else:
                    refund_rows.append({"Label": label, "RM": None, "Total Refund Revenue": None})

            else:
                nums = re.findall(r"[\d,.]+", line)
                if len(nums) < 2:
                    for j in range(1, 3):
                        if i + j < len(lines):
                            nums += re.findall(r"[\d,.]+", lines[i + j])
                        if len(nums) >= 2:
                            break

                if len(nums) >= 2:
                    val1 = float(nums[0].replace(",", ""))
                    val2 = float(nums[1].replace(",", ""))
                    if val1 <= 1.0 and val2 <= 1.0:
                        val1, val2 = None, None

                    if current_section == "by_tax":
                        tax_by_tax_rows.append({"Label": label, "T1": val1, "T5": val2})
                    elif current_section == "exempt_tax":
                        exempt_tax_rows.append({"Label": label, "T1": val1, "T5": val2})
                    elif current_section == "txn":
                        txn_rows.append({"Label": label, "RM": val1, "Total Tax Exempt Revenue": val2})

        i += 1

    df_tax_by_tax = pd.DataFrame(tax_by_tax_rows).set_index("Label") if tax_by_tax_rows else pd.DataFrame()
    df_exempt_tax = pd.DataFrame(exempt_tax_rows).set_index("Label") if exempt_tax_rows else pd.DataFrame()
    df_txn = pd.DataFrame(txn_rows).set_index("Label") if txn_rows else pd.DataFrame()
    df_refund = pd.DataFrame(refund_rows).set_index("Label") if refund_rows else pd.DataFrame()

    return df_tax_by_tax, df_exempt_tax, df_txn, df_refund, business_date


def extract_gross_room_revenue(camelot_tables, filename, business_date):
    try:
        for table in camelot_tables:
            df = table.df
            flat_text = " ".join(df.astype(str).values.flatten())
            if "ROOM CHARGE (RM)" in flat_text and "YTD Totals" in flat_text:
                header_idx = None
                for i, row in df.iterrows():
                    if "Today's Net" in row.tolist() and "YTD Totals" in row.tolist():
                        header_idx = i
                        break
                if header_idx is not None:
                    df_clean = df.iloc[header_idx + 1:].reset_index(drop=True)
                    df_clean.columns = ["description", "opening_balance", "today_total", "adjustments",
                                        "net", "monthly_total", "ytd_total"]
                    numeric_cols = ["opening_balance", "today_total", "adjustments", "net", "monthly_total", "ytd_total"]
                    for col in numeric_cols:
                        df_clean[col] = (df_clean[col].str.replace(",", "")
                                                      .str.replace("(", "-")
                                                      .str.replace(")", ""))
                        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

                    valid_rows = df_clean[
                        df_clean["opening_balance"].notna() &
                        ~df_clean["description"].str.lower().str.contains("date/time|^room$")
                    ].reset_index(drop=True)

                    valid_rows["source_file"] = filename
                    valid_rows["business_date"] = pd.to_datetime(business_date).date() if business_date else None
                    valid_rows["load_timestamp"] = datetime.now()
                    return valid_rows
    except Exception as e:
        logger.error(f"❌ Gross Room Revenue extraction failed: {e}\n{traceback.format_exc()}")
    return pd.DataFrame()


def extract_revenue_by_rate_code(camelot_tables, filename):
    try:
        ratecode_patterns = ['SRD', 'SAPR', 'SP3', 'BAR', 'LEXT', 'LCOM', 'LCLC', 'SNP', 'SSC', 'SO2BK', 'SGML']

        def combine_headers(row1, row2):
            return [(str(c1) + " " + str(c2)).strip().replace('  ', ' ') for c1, c2 in zip(row1, row2)]

        def make_headers_unique(headers):
            seen = {}
            unique_headers = []
            for h in headers:
                if h not in seen:
                    seen[h] = 1
                    unique_headers.append(h)
                else:
                    seen[h] += 1
                    unique_headers.append(f"{h}_{seen[h]}")
            return unique_headers

        revenue_tables = []
        standard_headers = None
        first_revenue_table = True

        for i, table in enumerate(camelot_tables):
            df = table.df
            if df.shape[1] < 8 or df.shape[0] < 5:
                continue

            first_col = df.iloc[:, 0].dropna().astype(str)
            ratecode_like_count = sum(any(code in val for code in ratecode_patterns) for val in first_col)

            if ratecode_like_count >= 3:
                if first_revenue_table:
                    header_row1 = df.iloc[2]
                    header_row2 = df.iloc[3]
                    headers = make_headers_unique(combine_headers(header_row1, header_row2))
                    df.columns = headers
                    df = df.drop(index=[0, 1, 2, 3]).reset_index(drop=True)
                    standard_headers = df.columns.tolist()
                    first_revenue_table = False
                else:
                    df.columns = standard_headers
                    df = df.drop(index=[0, 1]).reset_index(drop=True)
                revenue_tables.append(df)

        if revenue_tables:
            final_df = pd.concat(revenue_tables, ignore_index=True).dropna(how='all')
            final_df.columns = final_df.columns.str.strip()

            # Map expected MySQL columns
            df = pd.DataFrame({
                "source_file": filename,
                "rate_code": final_df.get('Rate Code'),
                "room_nights": final_df.get('Room Nights'),
                "room_nights_percent": final_df.get('%'),
                "room_revenue": final_df.get('Room Revenue'),
                "room_revenue_percent": final_df.get('%_2'),
                "daily_avg": final_df.get('Daily AVG'),
                "ptd_room_nights": final_df.get('PTD Room Nights'),
                "ptd_room_revenue": final_df.get('PTD Room Revenue'),
                "ptd_avg": final_df.get('PTD AVG'),
                "ytd_room_nights": final_df.get('Room Nights_2'),
                "ytd_room_revenue": final_df.get('YTD Room Revenue'),
                "ytd_avg": final_df.get('% YTD AVG'),
            })

            float_cols = [col for col in df.columns if col not in ['file_name', 'rate_code']]
            for col in float_cols:
                df[col] = df[col].apply(lambda x: safe_float(x))

            return df
        else:
            logger.warning(f"⚠️ No Revenue by Rate Code section found in {filename}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"❌ Revenue by Rate Code extraction failed for {filename}: {e}\n{traceback.format_exc()}")
        return pd.DataFrame()



def extract_advance_deposit_journal(list_of_pages):
    records = []
    current_transaction_type = None
    collecting = False 
    date_pattern = r"\d{1,2}/\d{1,2}/\d{2,4}"

    for page_idx, lines in enumerate(list_of_pages):
        for line_idx, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Advance Deposit Journal" in line:
                collecting = True
                continue

            if "Advance Deposit Ledger" in line:
                collecting = False
                continue

            if not collecting:
                continue

            # Detect Transaction Code
            transaction_match = re.search(r"Transaction Code:\s*(.+)", line)
            if transaction_match:
                current_transaction_type = transaction_match.group(1).strip()
                continue

            # Match deposit record
            match = re.match(rf"({date_pattern})\s+(\S+)\s+(\S+)\s+(\d+)\s+(.+?)\s+(\(?-?\d+\.?\d*\)?)$", line)
            if match:
                posting_date_raw, user_id, room_or_type, account_number, account_name, total = match.groups()

                # Parse date
                try:
                    posting_date_obj = datetime.strptime(posting_date_raw, "%m/%d/%y")
                except ValueError:
                    posting_date_obj = datetime.strptime(posting_date_raw, "%m/%d/%Y")
                posting_date = posting_date_obj.strftime("%Y-%m-%d")

                # Room and account type logic
                if room_or_type in ['Guest', 'DirectBill', 'Group', 'Other']:
                    room = None
                    account_type = room_or_type
                else:
                    room = room_or_type
                    account_type = None

                # Clean total
                total = float(total.replace('(', '-').replace(')', ''))

                records.append({
                    "posting_date": posting_date,
                    "user_id": user_id,
                    "room": room,
                    "account_type": account_type,
                    "account_number": account_number,
                    "account_name": account_name.strip(),
                    "total": total,
                    "transaction_type": current_transaction_type
                })

    return pd.DataFrame(records)

