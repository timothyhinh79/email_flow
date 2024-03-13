from lib.parse_helpers import parse_email_body, process_history
import uuid
import datetime
from dotenv import load_dotenv
import os

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
        'id': uuid.uuid5(uuid.NAMESPACE_DNS, '-'.join(['4.99', 'February 28, 2024', 'PAYPAL TWITCHINTER'])),
        'transaction_type': 'credit',
        'amount': '4.99',
        'transaction_date': 'February 28, 2024',
        'description': 'PAYPAL TWITCHINTER',
        'category': None
    }

    assert updated_at_timestamp >= timestamp_before_parsing

def test_process_history():

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_CLIENT_ID, 
        'client_secret': GOOGLE_CLIENT_SECRET,
        'refresh_token': GOOGLE_REFRESH_TOKEN
    })

    gmail = build('gmail', 'v1', credentials=creds)

    from tests.data.sample_history import history

    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )

    start_timestamp = datetime.datetime.now(datetime.timezone.utc)
    db_records = process_history(
        gmail_client = gmail,
        history=history,
        label_id="Label_3884943773140766149",
        save_to_db_=False,
        db_creds=db_creds,
    )

    updated_at_timestamps = []
    for db_record in db_records:
        db_record['id'] = str(db_record['id'])   
        updated_at_timestamps.append(db_record['updated_at'])
        db_record.pop('updated_at')
    
    expected_db_records = [
        {'id': '3aa2eb80-93bb-5684-bee9-b58d9f41c026', 'transaction_type': 'credit', 'amount': '4.99', 'transaction_date': 'February 28, 2024', 'description': 'PAYPAL TWITCHINTER', 'category': None},
        {'id': '3aa2eb80-93bb-5684-bee9-b58d9f41c026', 'transaction_type': 'credit', 'amount': '4.99', 'transaction_date': 'February 28, 2024', 'description': 'PAYPAL TWITCHINTER', 'category': None}, 
        {'id': '3aa2eb80-93bb-5684-bee9-b58d9f41c026', 'transaction_type': 'credit', 'amount': '4.99', 'transaction_date': 'February 28, 2024', 'description': 'PAYPAL TWITCHINTER', 'category': None}, 
        {'id': '3aa2eb80-93bb-5684-bee9-b58d9f41c026', 'transaction_type': 'credit', 'amount': '4.99', 'transaction_date': 'February 28, 2024', 'description': 'PAYPAL TWITCHINTER', 'category': None}, 
        {'id': '3aa2eb80-93bb-5684-bee9-b58d9f41c026', 'transaction_type': 'credit', 'amount': '4.99', 'transaction_date': 'February 28, 2024', 'description': 'PAYPAL TWITCHINTER', 'category': None}
    ]

    
    assert db_records == expected_db_records

    for updated_at_timestamp in updated_at_timestamps:
        assert updated_at_timestamp >= start_timestamp
