from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
import email
from lib.gmail.gmail import compose_and_send_email

from dotenv import load_dotenv
import os

load_dotenv()

GOOGLE_GMAIL_CLIENT_ID = os.getenv('GOOGLE_GMAIL_CLIENT_ID')
GOOGLE_GMAIL_CLIENT_SECRET = os.getenv('GOOGLE_GMAIL_CLIENT_SECRET')
GOOGLE_GMAIL_REFRESH_TOKEN = os.getenv('GOOGLE_GMAIL_REFRESH_TOKEN')

def test_compose_and_send_email():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    sent_message = compose_and_send_email(
        service = gmail,
        sender='tim098292@gmail.com',
        to='tim098292@gmail.com',
        subject='Pytest Email',
        body='Testing compose_and_send_email() in pytest'
    )

    message = gmail.users().messages().get(userId='me', id=sent_message['id'], format = 'raw').execute()
    mime_msg = email.message_from_bytes(base64.urlsafe_b64decode(message['raw']))
    
    assert mime_msg['subject'] == 'Pytest Email'