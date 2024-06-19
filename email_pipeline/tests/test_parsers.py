import uuid
import datetime
from dotenv import load_dotenv
import os

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from email_processing.parsers import (
    parse_credit_card_transaction,
    process_financial_transaction_message,
    parse_zelle_transfer,
    parse_direct_deposit
)

load_dotenv()

GOOGLE_PRIMARY_GMAIL_CLIENT_ID = os.getenv('GOOGLE_PRIMARY_GMAIL_CLIENT_ID')
GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET = os.getenv('GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET')
GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN = os.getenv('GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')


def test_parse_credit_card_transaction():
    example_bofa_email = """
        From: Bank of America <onlinebanking@ealerts.bankofamerica.com>
        Date: Wed, Feb 28, 2024, 18:07
        Subject: Credit card transaction exceeds alert limit you set
        To: <timothyhinh79@gmail.com>


        [image: Bank of America.]
        Credit card transaction exceeds alert limit you set
        Customized Cash Rewards Visa Signature* ending in 1234*
        Amount: *$4,000.99*
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

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = parse_credit_card_transaction(example_bofa_email)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        # 'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(['4.99', 'February 28, 2024', 'PAYPAL TWITCHINTER'])),
        'transaction_type': 'credit',
        'amount': 4000.99,
        'transaction_date': 'February 28, 2024',
        'description': 'PAYPAL TWITCHINTER',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing


def test_parse_credit_card_transaction_on_unforwarded_email_with_html():
    with open('./tests/data/sample_html_payload.txt', 'r') as f:
        html_payload = f.read()

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = parse_credit_card_transaction(html_payload)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        # 'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(['7.99', 'March 07, 2024', 'Hulu 877-8244858 CA'])),
        'transaction_type': 'credit',
        'amount': 7.99,
        'transaction_date': 'March 07, 2024',
        'description': 'Hulu 877-8244858 CA',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing

def test_parse_zelle_transfer():
    with open('./tests/data/sample_zelle_transfer_email_body.txt', 'r') as f:
        zelle_email_payload = f.read()

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = parse_zelle_transfer(zelle_email_payload)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        'transaction_type': 'credit',
        'amount': 41.00,
        'description': 'Fuego Cravings',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing    

def test_parse_direct_deposit():
    with open('./tests/data/sample_direct_dep_email_body.txt', 'r') as f:
        direct_deposit_email_payload = f.read()

    timestamp_before_parsing = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = parse_direct_deposit(direct_deposit_email_payload)

    data_json_wo_time = {k:v for k,v in data_json.items() if k != 'updated_at'}
    updated_at_timestamp = data_json['updated_at']

    assert data_json_wo_time == {
        'transaction_type': 'debit',
        'amount': 101.00,
        'transaction_date': 'March 19, 2024',
        'description': 'Sender: John Doe',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing 


def test_process_message_with_credit_card_transaction():

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    start_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = process_financial_transaction_message(gmail, message_id = '18e362f2ae442dc7', save_to_db_ = False)
    updated_at_timestamp = data_json.pop('updated_at')
    
    assert data_json == {
        'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, '18e362f2ae442dc7')),
        'message_id': '18e362f2ae442dc7',
        'transaction_type': 'credit', 
        'amount': 4.99, 
        'transaction_date': 'March 13, 2024', 
        'description': 'PAYPAL  TWITCHINTER', 
        'category': None
    }

    assert updated_at_timestamp >= start_timestamp


def test_process_message_with_zelle_transfer():

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    start_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = process_financial_transaction_message(gmail, message_id = '18e82be57a7be56e', save_to_db_ = False)
    updated_at_timestamp = data_json.pop('updated_at')
    
    assert data_json == {
        'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, '18e82be57a7be56e')),
        'message_id': '18e82be57a7be56e',
        'transaction_type': 'credit', 
        'amount': 1.00, 
        'transaction_date': 'March 28, 2024', # datetime.datetime(2024, 3, 28, 1, 48, 16), 
        'description': 'Test', 
        'category': None
    }

    assert updated_at_timestamp >= start_timestamp


def test_process_message_with_direct_deposit():

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_PRIMARY_GMAIL_CLIENT_ID, 
        'client_secret': GOOGLE_PRIMARY_GMAIL_CLIENT_SECRET,
        'refresh_token': GOOGLE_PRIMARY_GMAIL_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    start_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_json = process_financial_transaction_message(gmail, message_id = '18e55a5b8ebf4038', save_to_db_ = False)
    updated_at_timestamp = data_json.pop('updated_at')
    
    assert data_json == {
        'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, '18e55a5b8ebf4038')),
        'message_id': '18e55a5b8ebf4038',
        'transaction_type': 'debit', 
        'amount': 473.00, 
        'transaction_date': 'March 19, 2024', 
        'description': 'Sender: FRANCHISE TAX BD CASTTAXRFD', 
        'category': None
    }

    assert updated_at_timestamp >= start_timestamp
