
from google.cloud import secretmanager
from google.oauth2 import service_account
import json
import re
import uuid
import datetime
import base64
import email

from models.financial_transaction import FinancialTransaction
from db_utils.db_functions import save_to_db

def access_secret_version(project_id, secret_id, version_id, service_account_file = None):

    # Create Secret Manager client.
    if service_account_file:
        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client = secretmanager.SecretManagerServiceClient(credentials = credentials)
    else:
        client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload.
    return json.loads(response.payload.data.decode('UTF-8'))

def parse_email_body(email_body):
    amount_pattern = r"Amount:.*?\*\$([\d,]+\.\d{2})\*"
    date_pattern = r"Date:.*?\*(\w+\s\d{2},\s\d{4})\*"
    where_pattern = r"Where:\s*\*(.*?)\*"
    
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

    transaction_type = 'credit'

    data_json = {
        'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join([amount, date, where])),
        'transaction_type': transaction_type,
        'amount': amount,
        'transaction_date': date,
        'description': where,
        'category': None,
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }
    
    return data_json

def process_history(gmail_client, history, label_id = None, save_to_db_ = True, db_creds = None):

    db_records = []

    for history_record in history['history']:
        if 'messagesAdded' in history_record:
            for messageAdded in history_record['messagesAdded']:
                
                message = messageAdded['message']
                if 'labelIds' in message and (label_id == None or label_id in message['labelIds']):

                    # Grab message from Gmail API in raw format and decode
                    msg = gmail_client.users().messages().get(userId='me', id=message['id'], format='raw').execute()
                    mime_msg = email.message_from_bytes(base64.urlsafe_b64decode(msg['raw']))

                    # Get email subject
                    subject = mime_msg['subject']
                    assert subject, f"No subject found in message with id {message['id']}"

                    # For credit card transaction email alerts, grab desired data
                    if subject == 'Credit card transaction exceeds alert limit you set':

                        # Extract full message body
                        body = ''
                        message_main_type = mime_msg.get_content_maintype()
                        if message_main_type == 'multipart':
                            for part in mime_msg.get_payload():
                                if part.get_content_maintype() == 'text':
                                    body += part.get_payload()
                        elif message_main_type == 'text':
                            body = mime_msg.get_payload()

                        # Parse full message body and create db record
                        data_json = parse_email_body(body)
                        db_records.append(data_json)

                        if save_to_db_:
                            save_to_db(FinancialTransaction, data_json, db_creds=db_creds)                        
    
    return db_records
