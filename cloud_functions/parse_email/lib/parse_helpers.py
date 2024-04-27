from google.cloud import secretmanager
from google.oauth2 import service_account
import json
import re
import uuid
import datetime
import base64
import email
from email.utils import parsedate_to_datetime

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
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }
    
    return data_json


def parse_zelle_transfer(email_body):

    # Find description of Zelle payment and the associated message
    transfer_desc_pattern = r"You sent \$([\d,]+\.\d{2}) to ([\w\s]+?)[\s]*</td>"
    message_pattern = r"Your message.*?<b>\s*([\w\s]+?)\s*</b>"

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
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }
    
    return data_json


def parse_direct_deposit(email_body):

    # Find description of Zelle payment and the associated message
    amount_pattern = r"Amount:.*?<td.*?>\s*\$\s*</td>\s*<td.*?>\s*([\d,]+\.\d{2})\s*</td>"
    transaction_date_pattern = r"On:.*?<td.*?>\s*(\w+\s\d{2},\s\d{4})\s*</td>"
    sender_pattern = r"From:.*?<td.*?>\s*([\w\s]+?)\s*</td>"

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
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }
    
    return data_json


def get_date_received(full_message):

    headers = full_message['payload']['headers']
    
    # Extract the receipt date from the first header with name = 'Received'
    # See following SO post for more details
    # https://stackoverflow.com/questions/76260823/what-is-the-correct-timestamp-to-use-when-querying-gmail-api-by-epoch
    for header in headers:
        if header['name'] == 'Received':

            text = header['value']
            pattern = r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} -\d{4} \(\w{3}\)'
            
            # parse text for date received and convert to Unix timestamp (# of seconds since January 1st, 1970)
            match = re.search(pattern, text)
            assert match, f"Unable to find date received for message with id {full_message['id']}"

            # Convert date string to unix time
            date_string = match.group()
            dt = parsedate_to_datetime(date_string)
            unix_time = dt.timestamp()
            return int(unix_time)
    
    assert False, f"Did not find header with name 'Received' for message with id {full_message['id']}"


def get_messages_after_specific_message(
    gmail_client, 
    message_id = None, 
    label_ids = []
):
    
    # If no message id is provided, get all messages associated with provided label_id
    if not message_id:
        results = gmail_client.users().messages().list(userId='me', labelIds=label_ids).execute()
        messages = results.get('messages', [])

    # Otherwise, get messages that occur after the provided message
    else:

        # Get the date received of the specified message
        message = gmail_client.users().messages().get(userId='me', id=message_id).execute()
        date_received_for_comparison = get_date_received(message)

        # Get the list of messages
        # Adding 60 second buffer to ensure we don't miss any emails
        # We will get extra emails that may already be in the database (included the provided message), but save_to_db() should not add duplicate records for the same message id
        results = gmail_client.users().messages().list(
            userId='me', 
            labelIds=label_ids,q=f'after:{int(date_received_for_comparison) - 60}'
        ).execute()

    messages = results.get('messages', [])

    return messages

def process_message(gmail_client, message_id, save_to_db_ = True, db_creds = None):

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
        data_json['id'] = uuid.uuid5(uuid.NAMESPACE_DNS, message_id) 

        if save_to_db_:

            save_to_db(FinancialTransaction, data_json, db_creds=db_creds)  
            print(f"Successfully saved record to database for message with id {message_id}")

    return data_json
    

def get_latest_message_id(gmail_client, messages):

    assert messages, "No messages provided, cannot get latest message ID"

    # Retrieve full message details and sort by date received
    full_messages = []
    for message in messages:
        msg = gmail_client.users().messages().get(userId='me', id=message['id']).execute()
        full_messages.append(msg)

    sorted_messages = sorted(full_messages, key=lambda msg: get_date_received(msg))
    return sorted_messages[-1]['id']
