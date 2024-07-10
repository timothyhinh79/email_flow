from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
import base64
import re

def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}

def send_message(service, user_id, message):
    message = (service.users().messages().send(userId=user_id, body=message).execute())
    print('Message Id: %s' % message['id'])
    return message

def compose_and_send_email(google_creds, sender, to, subject, body):
    service = build('gmail', 'v1', credentials=google_creds)
    message = create_message(sender, to, subject, body)
    sent_message = send_message(service, "me", message)
    return sent_message

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
        results = gmail_client.users().messages().list(
            userId='me', 
            labelIds=label_ids,q=f'after:{int(date_received_for_comparison)}'
        ).execute()

    messages = results.get('messages', [])

    return messages
    

def get_latest_message_id(gmail_client, messages):

    assert messages, "No messages provided, cannot get latest message ID"

    # Retrieve full message details and sort by date received
    full_messages = []
    for message in messages:
        msg = gmail_client.users().messages().get(userId='me', id=message['id']).execute()
        full_messages.append(msg)

    sorted_messages = sorted(full_messages, key=lambda msg: get_date_received(msg))
    return sorted_messages[-1]['id']
