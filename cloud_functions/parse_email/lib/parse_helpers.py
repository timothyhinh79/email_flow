
from google.cloud import secretmanager
from google.oauth2 import service_account
import json
import re
import uuid
import datetime
import base64

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
    amount_pattern = r"Amount:\s*\*\$(\d+\.\d{2})\*"
    date_pattern = r"Date:\s*\*(\w+\s\d{2},\s\d{4})\*"
    where_pattern = r"Where:\s*\*(.*)\*"
    
    amount_search = re.search(amount_pattern, email_body)
    date_search = re.search(date_pattern, email_body)
    where_search = re.search(where_pattern, email_body)

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
                    msg = gmail_client.users().messages().get(userId='me', id=message['id']).execute()

                    # Extract the headers
                    headers = msg['payload']['headers']

                    # Find the subject header
                    subject = None
                    for header in headers:
                        if header['name'] == 'Subject':
                            # Print the subject
                            subject = header['value']

                    assert subject, f"No subject found in message with id {message['id']}"

                    if subject == 'Credit card transaction exceeds alert limit you set':
                        if 'data' in msg['payload']['body']:
                            data = msg['payload']['body']['data']
                        else:
                            # if email is multipart, get the first part, which is typically the plain text version of the email body
                            parts = msg['payload'].get('parts')[0]
                            data = parts['body']['data']

                    
                        # Decode the body data
                        data = data.replace("-","+").replace("_","/")
                        decoded_data = base64.urlsafe_b64decode(data)
                        body = decoded_data.decode("utf-8")

                        data_json = parse_email_body(body)
                        db_records.append(data_json)

                        if save_to_db_:
                            save_to_db(FinancialTransaction, data_json, db_creds=db_creds)                        
    
    return db_records
