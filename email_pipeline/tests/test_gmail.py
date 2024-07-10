from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
import email
from src.services.gmail.gmail import (
    compose_and_send_email, 
    get_date_received, 
    get_latest_message_id, 
    get_messages_after_specific_message
)

from dotenv import load_dotenv
import os

load_dotenv()

GOOGLE_PRIMARY_GMAIL_CLIENT_ID = os.getenv('GOOGLE_PRIMARY_GMAIL_CLIENT_ID')
GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET = os.getenv('GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET')
GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN = os.getenv('GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN')
GOOGLE_DUMMY_GMAIL_CLIENT_ID = os.getenv('GOOGLE_DUMMY_GMAIL_CLIENT_ID')
GOOGLE_DUMMY_GMAIL_CLIENT_SECRET = os.getenv('GOOGLE_DUMMY_GMAIL_CLIENT_SECRET')
GOOGLE_DUMMY_GMAIL_REFRESH_TOKEN = os.getenv('GOOGLE_DUMMY_GMAIL_REFRESH_TOKEN')

def sort_email_messages(gmail_client, messages):
    full_messages = []
    for message in messages:
        msg = gmail_client.users().messages().get(userId='me', id=message['id']).execute()
        full_messages.append(msg)

    message_ids_n_dates = [
        {'id': full_message['id'], 'date_received': get_date_received(full_message)}
        for full_message in full_messages
    ]

    sorted_messages = sorted(message_ids_n_dates, key=lambda msg: int(msg['date_received']))
    return sorted_messages

def test_compose_and_send_email():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_DUMMY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_DUMMY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_DUMMY_GMAIL_REFRESH_TOKEN
    })

    sent_message = compose_and_send_email(
        google_creds=creds,
        sender='tim098292@gmail.com',
        to='tim098292@gmail.com',
        subject='Pytest Email',
        body='Testing compose_and_send_email() in pytest'
    )

    gmail = build('gmail', 'v1', credentials=creds)
    message = gmail.users().messages().get(userId='me', id=sent_message['id'], format = 'raw').execute()
    mime_msg = email.message_from_bytes(base64.urlsafe_b64decode(message['raw']))
    
    assert mime_msg['subject'] == 'Pytest Email'

def test_get_date_received():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    full_message = gmail.users().messages().get(userId='me', id='18e71fe33ef1b7aa').execute()
    date_received = get_date_received(full_message)
    assert date_received == 1711309468

def test_get_latest_message_id():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    messages = [
        {'id': '18dced81bdd8b94e'}, 
        {'id': '18dcef1c5b9281d1'}, 
        {'id': '18e362f2ae442dc7'},
        {'id': '18e4f00dd55eb102'} # this is the message with the latest InternalDate
    ]

    latest_message_id = get_latest_message_id(gmail, messages)
    
    assert latest_message_id == '18e4f00dd55eb102'

def test_get_latest_message_id_when_no_messages_are_provided():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    messages = []

    # Check if get_latest_message_id() throws an exception if 'messages' is empty
    try:
        latest_message_id = get_latest_message_id(gmail, messages)
        assert False, "get_latest_message_id() did not throw an exception when 'messages' is empty"
    except: 
        assert True

def test_get_messages_after_specific_message_with_no_message_id():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    # Get messages for "Testing for Email Parser" label
    messages = get_messages_after_specific_message(gmail, label_ids = ['Label_7814975169765856594'])
    temp = []
    for message in messages:
        full_message = gmail.users().messages().get(userId='me', id=message['id']).execute()
        temp.append((message['id'], get_date_received(full_message)))
    
    sorted_messages = sort_email_messages(gmail, messages)
    
    # As of 3/20/24, this label should only have 4 messages
    expected_messages = [
        {'id': '18dced81bdd8b94e', 'date_received': 1708572286}, 
        {'id': '18dcef1c5b9281d1', 'date_received': 1708573967}, 
        {'id': '18e362f2ae442dc7', 'date_received': 1710306045},
        {'id': '18e4f00dd55eb102', 'date_received': 1710722440}
    ]
    
    assert sorted_messages == expected_messages


def test_get_messages_after_specific_message_with_message_id():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    # Get messages for "Testing for Email Parser" label after the first message
    messages = get_messages_after_specific_message(
        gmail, 
        message_id = '18dcef1c5b9281d1',
        label_ids = ['Label_7814975169765856594']
    )
    sorted_messages = sort_email_messages(gmail, messages)
    
    # We should get messages appearing after 18dcef1c5b9281d1 AND message 18dcef1c5b9281d1 as well (due to 60-second buffer)
    # It's okay to get the same message, because save_to_db() will not add duplicate transaction records
    expected_messages = [
        {'id': '18dcef1c5b9281d1', 'date_received': 1708573967}, 
        {'id': '18e362f2ae442dc7', 'date_received': 1710306045},
        {'id': '18e4f00dd55eb102', 'date_received': 1710722440}
    ]
    
    assert sorted_messages == expected_messages