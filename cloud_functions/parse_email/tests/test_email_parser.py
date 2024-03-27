from lib.parse_helpers import (
    parse_email_body, 
    get_messages_after_specific_message, 
    process_message, 
    get_latest_message_id,
    get_date_received
)
import uuid
import datetime
from dotenv import load_dotenv
import os
import json

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from classes.db_credentials import DBCredentials

load_dotenv()

GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.getenv('GOOGLE_REFRESH_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

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

def test_get_date_received():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    full_message = gmail.users().messages().get(userId='me', id='18e71fe33ef1b7aa').execute()
    date_received = get_date_received(full_message)
    assert date_received == 1711309468


def test_parse_email_body():
    example_bofa_email = """
        From: Bank of America <onlinebanking@ealerts.bankofamerica.com>
        Date: Wed, Feb 28, 2024, 18:07
        Subject: Credit card transaction exceeds alert limit you set
        To: <timothyhinh79@gmail.com>


        [image: Bank of America.]
        Credit card transaction exceeds alert limit you set
        Customized Cash Rewards Visa Signature* ending in 3057*
        Amount: *$4.99*
        Date: *February 28, 2024*
        Where: *PAYPAL TWITCHINTER*
        View details
        <https://www.bankofamerica.com/deeplink/redirect.go?target=bofasignin&screen=Accounts:Home&version=7.0.0>
        If you made this purchase or payment but don't recognize the amount, wait
        until the final purchase amount has posted before filing a dispute claim.
        If you don't recognize this activity, please contact us at the number on
        the back of your card.
        *Did you know?*
        You can choose how you get alerts from us including text messages and
        mobile notifications. Go to Alert Settings
        <https://www.bankofamerica.com/deeplink/redirect.go?target=alerts_settings&screen=Alerts:Home&gotoSetting=true&version=7.1.0> 
        We'll never ask for your personal information such as SSN or ATM PIN in
        email messages. If you get an email that looks suspicious or you are not
        the intended recipient of this email, don't click on any links. Instead,
        forward to abuse@bankofamerica.com
        <#m_-8487647731247882732_m_-6156157492217928947_> then delete it.
        Please don't reply to this automatically generated service email.
        Privacy Notice
        <https://www.bankofamerica.com/privacy/consumer-privacy-notice.go> Equal
        Housing Lender <https://www.bankofamerica.com/help/equalhousing.cfm>
        Bank of America, N.A. Member FDIC
        © 2024 Bank of America Corporation
    """

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc)
    data_json = parse_email_body(example_bofa_email)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        # 'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(['4.99', 'February 28, 2024', 'PAYPAL TWITCHINTER'])),
        'transaction_type': 'credit',
        'amount': '4.99',
        'transaction_date': 'February 28, 2024',
        'description': 'PAYPAL TWITCHINTER',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing


def test_parse_email_body_on_unforwarded_email_with_html():
    with open('./tests/data/sample_html_payload.txt', 'r') as f:
        html_payload = f.read()

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc)
    data_json = parse_email_body(html_payload)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        # 'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(['7.99', 'March 07, 2024', 'Hulu 877-8244858 CA'])),
        'transaction_type': 'credit',
        'amount': '7.99',
        'transaction_date': 'March 07, 2024',
        'description': 'Hulu 877-8244858 CA',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing


def test_get_messages_after_specific_message_with_no_message_id():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
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
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    # Get messages for "Testing for Email Parser" label after the first message
    messages = get_messages_after_specific_message(
        gmail, 
        message_id = '18dced81bdd8b94e',
        label_ids = ['Label_7814975169765856594']
    )
    sorted_messages = sort_email_messages(gmail, messages)
    
    # We should only see the latter two messages from the prior test
    expected_messages = [
        {'id': '18dcef1c5b9281d1', 'date_received': 1708573967}, 
        {'id': '18e362f2ae442dc7', 'date_received': 1710306045},
        {'id': '18e4f00dd55eb102', 'date_received': 1710722440}
    ]
    
    assert sorted_messages == expected_messages


def test_process_message():
# def process_message(gmail_client, message_id, save_to_db_ = True, db_creds = None):
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    start_timestamp = datetime.datetime.now(datetime.timezone.utc)
    data_json = process_message(gmail, message_id = '18e362f2ae442dc7', save_to_db_ = False)
    updated_at_timestamp = data_json.pop('updated_at')
    
    assert data_json == {
        'id': uuid.uuid5(uuid.NAMESPACE_DNS, '18e362f2ae442dc7'),
        'message_id': '18e362f2ae442dc7',
        'transaction_type': 'credit', 
        'amount': '4.99', 
        'transaction_date': 'March 13, 2024', 
        'description': 'PAYPAL  TWITCHINTER', 
        'category': None
    }

    assert updated_at_timestamp >= start_timestamp


def test_get_latest_message_id():
    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
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
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    messages = []

    # Check if get_latest_message_id() throws an exception if 'messages' is empty
    try:
        latest_message_id = get_latest_message_id(gmail, messages)
        assert False, "get_latest_message_id() did not throw an exception when 'messages' is empty"
    except: 
        assert True
