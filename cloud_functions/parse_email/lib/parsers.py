import re
import uuid
import datetime
import base64
import email

from models.financial_transaction import FinancialTransaction
from db_utils.db_functions import save_to_db
from lib.gmail.gmail import get_date_received

def parse_credit_card_transaction(email_body):

    # Find details on the amount, transaction date, and "where" transaction occurred
    amount_pattern = r"Amount:.*?\*\$([\d,]+\.\d{2})\*"
    date_pattern = r"Date:.*?\*(\w+\s\d{2},\s\d{4})\*"
    where_pattern = r"Where:\s*\*(.*?)\*"
    
    # re.DOTALL flag ensures that '.' pattern matches newlines
    amount_search = re.search(amount_pattern, email_body, re.DOTALL)
    date_search = re.search(date_pattern, email_body, re.DOTALL)
    where_search = re.search(where_pattern, email_body, re.DOTALL)

    if not amount_search:
        amount_pattern = r"Amount:.*?<b>\$([\d,]+\.\d{2})</b>"
        amount_search = re.search(amount_pattern, email_body, re.DOTALL)

    if not date_search:
        date_pattern = r"Date:.*?<b>(\w+\s\d{2},\s\d{4})</b>"
        date_search = re.search(date_pattern, email_body, re.DOTALL)

    if not where_search:
        where_pattern = r"Where:.*?<b>(.*?)</b>"
        where_search = re.search(where_pattern, email_body, re.DOTALL)

    assert amount_search and date_search and where_search, "Amount + Date + Where not found in credit card transaction email from BofA"

    amount = amount_search.group(1)
    date = date_search.group(1)
    where = where_search.group(1)

    # Set up data_json with relevant info to save to database
    data_json = {
        'transaction_type': 'credit',
        'amount': float(amount.replace(',','')),
        'transaction_date': date,
        'description': where,
        'category': None,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return data_json


def parse_zelle_transfer(email_body):

    # Find description of Zelle payment and the associated message
    transfer_desc_pattern = r"You sent \$([\d,]+\.\d{2}) to (.+?)[\s]*</td>"
    message_pattern = r"Your message.*?<b>\s*(.+?)\s*</b>"

    transfer_desc_search = re.search(transfer_desc_pattern, email_body, re.DOTALL)
    message_search = re.search(message_pattern, email_body, re.DOTALL)
    assert transfer_desc_search and message_search, "Unable to find ""You sent $... to ..."" and message description within Zelle transfer email"

    amount = transfer_desc_search.group(1)
    recipient = transfer_desc_search.group(2) # TODO: figure out where to save this info
    message = message_search.group(1)

    # Set up data_json with relevant info to save to database
    data_json = {
        'transaction_type': 'credit',
        'amount': float(amount.replace(',','')),
        'description': message,
        'category': None,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return data_json


def parse_direct_deposit(email_body):

    # Find description of Zelle payment and the associated message
    amount_pattern = r"Amount:.*?<td.*?>\s*\$\s*</td>\s*<td.*?>\s*([\d,]+\.\d{2})\s*</td>"
    transaction_date_pattern = r"On:.*?<td.*?>\s*(\w+\s\d{2},\s\d{4})\s*</td>"
    sender_pattern = r"From:.*?<td.*?>\s*(.+?)\s*</td>"

    amount_search = re.search(amount_pattern, email_body, re.DOTALL)
    transaction_date_search = re.search(transaction_date_pattern, email_body, re.DOTALL)
    sender_search = re.search(sender_pattern, email_body, re.DOTALL)
    assert amount_search and transaction_date_search and sender_search, "Unable to find amount, transaction_date, and/or sender within Direct Deposit email"

    amount = amount_search.group(1)
    transaction_date = transaction_date_search.group(1)
    sender = sender_search.group(1)

    # Set up data_json with relevant info to save to database
    data_json = {
        'transaction_type': 'debit',
        'amount': float(amount.replace(',','')),
        'transaction_date': transaction_date,
        'description': f'Sender: {sender}',
        'category': None,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return data_json


def process_financial_transaction_message(gmail_client, message_id, save_to_db_ = True, db_creds = None):

    message = gmail_client.users().messages().get(userId='me', id=message_id, format = 'raw').execute()
    mime_msg = email.message_from_bytes(base64.urlsafe_b64decode(message['raw']))

    # Get email subject
    subject = mime_msg['subject']
    assert subject, f"No subject found in message with id {message['id']}"

    # Extract full message body
    body = ''
    message_main_type = mime_msg.get_content_maintype()
    if message_main_type == 'multipart':
        for part in mime_msg.get_payload():
            if part.get_content_maintype() == 'text':
                body += part.get_payload()
    elif message_main_type == 'text':
        body = mime_msg.get_payload()
    
    data_json = None

    # Grab desired data from each type of email
    
    # Credit card transaction email alerts
    if subject == 'Credit card transaction exceeds alert limit you set':

        # Parse full message body and create db record
        data_json = parse_credit_card_transaction(body) 
    
    # Zelle transfers
    elif re.search(r"You sent \$[\d,]+\.\d{2} to ", subject, re.DOTALL):

        data_json = parse_zelle_transfer(body)

        # Get timestamp of when email was received and used that as the transaction date
        full_message = gmail_client.users().messages().get(userId='me', id=message_id).execute()
        data_json['transaction_date'] = datetime.datetime.utcfromtimestamp(get_date_received(full_message)).strftime("%B %d, %Y")

    # Direct Deposits
    elif subject == 'Receipt: Direct Deposit Received':

        data_json = parse_direct_deposit(body)

    # Finalize data_json and save to database
    if data_json:

        # add message ID and UUID
        data_json['message_id'] = message_id
        data_json['id'] = str(uuid.uuid5(uuid.NAMESPACE_DNS, message_id) )

        if save_to_db_:

            save_to_db(FinancialTransaction, data_json, db_creds=db_creds)  
            print(f"Successfully saved record to database for message with id {message_id}")

    return data_json
