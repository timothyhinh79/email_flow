from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText
import base64

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


