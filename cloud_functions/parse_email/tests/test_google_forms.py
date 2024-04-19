from lib.google_forms.common import (
    create_google_form,
    create_google_form_watch,
)
from lib.google_forms.transaction_categorization.categorize_transaction_question import generate_transaction_categorization_question
from lib.google_forms.transaction_categorization.process_categorization_submission import process_categorization_submission
from db_utils.db_functions import DBCredentials, query
from models.financial_transaction import FinancialTransactionTest

from google.oauth2.credentials import Credentials
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pytest
import datetime

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')
GOOGLE_FORM_CLIENT_ID = os.getenv('GOOGLE_FORM_CLIENT_ID')
GOOGLE_FORM_CLIENT_SECRET = os.getenv('GOOGLE_FORM_CLIENT_SECRET')
GOOGLE_FORM_REFRESH_TOKEN = os.getenv('GOOGLE_FORM_REFRESH_TOKEN')

@pytest.fixture()
def db_setup():
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}')

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS financial_transactions_test;
        CREATE TABLE financial_transactions_test (
                id VARCHAR primary key,
                message_id VARCHAR,
                transaction_type VARCHAR,
                amount FLOAT,
                transaction_date TIMESTAMPTZ,
                description VARCHAR,
                category VARCHAR,
                updated_at TIMESTAMPTZ
        );

        INSERT INTO financial_transactions_test VALUES
        ('record-1', 'message_id_1', 'debit', 100.0, '2024-01-01 00:00:05-08', 'Sample Description', 'Sample Category', '2024-01-02 00:00:12-08');
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)
        
    yield result

    # Define your SQL query
    query = text("""
        DROP TABLE IF EXISTS financial_transactions_test;
    """)

    # Execute the query and fetch all results
    with engine.connect() as connection:
        result = connection.execute(query)   

def test_create_google_form_for_transaction_categorization():
    question = generate_transaction_categorization_question(
        record_id='record-1',
        message_id='message-1',
        transaction_type='credit',
        transaction_date='April 1st, 2024',
        description='Rent',
        amount=1000.0
    )

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_FORM_CLIENT_ID, 
        'client_secret': GOOGLE_FORM_CLIENT_SECRET,
        'refresh_token': GOOGLE_FORM_REFRESH_TOKEN
    })

    form_result = create_google_form(
        google_creds=creds,
        google_form_title='Sample Form for Pytest',
        google_form_document_title='Categorize Financial Transaction',
        google_form_questions=question
    )

    assert form_result['info'] == {'title': 'Sample Form for Pytest', 'documentTitle': 'Categorize Financial Transaction'}

    form_questions = form_result['items']
    assert len(form_questions) == 1
    assert form_questions[0]['title'] == 'A transaction was recorded with the following details. Please assign an appropriate category.\tRecord ID: "record-1"; Message ID: "message-1"; Transaction Type: "credit"; Transaction Date: "April 1st, 2024"; Description: "Rent"; Amount: "1000.0"'


def test_create_google_form_watch():
    question = generate_transaction_categorization_question(
        record_id='record-1',
        message_id='message-1',
        transaction_type='credit',
        transaction_date='April 1st, 2024',
        description='Rent',
        amount=1000.0
    )

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_FORM_CLIENT_ID, 
        'client_secret': GOOGLE_FORM_CLIENT_SECRET,
        'refresh_token': GOOGLE_FORM_REFRESH_TOKEN
    })

    form_result = create_google_form(
        google_creds=creds,
        google_form_title='Sample Form for Pytest',
        google_form_document_title='Categorize Financial Transaction',
        google_form_questions=question
    )

    watch_result = create_google_form_watch(
        google_creds=creds, 
        form_id=form_result['formId'],
        event_type='RESPONSES',
        topic_name='projects/email-parser-414818/topics/categorize-transactions-form-submissions'
    )

    assert watch_result['target'] == {'topic': {'topicName': 'projects/email-parser-414818/topics/categorize-transactions-form-submissions'}}
    assert watch_result['eventType'] == 'RESPONSES'
    assert watch_result['state'] == 'ACTIVE'


def test_process_categorization_submission(db_setup):

    # Set up DB credentials
    db_creds = DBCredentials(
        host = DB_HOST,
        port = DB_PORT,
        user = DB_USER,
        password = DB_PASSWORD,
        database = DB_DATABASE
    )

    # Create credentials
    google_creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_FORM_CLIENT_ID, 
        'client_secret': GOOGLE_FORM_CLIENT_SECRET,
        'refresh_token': GOOGLE_FORM_REFRESH_TOKEN
    })

    form_response = process_categorization_submission(
        google_creds, 
        db_creds,
        FinancialTransactionTest,
        '1LKFq_n0CjL9SvqRbeSmmQS2IlDRCitXmTDjpoSL54Y8'
    )
    
    assert form_response == {
        'record_id': 'record-1',
        'category': 'Entertainment'
    }

    records = query(
        sql = 'SELECT * FROM financial_transactions_test', 
        db_creds = db_creds,
    )
    
    assert records == [

        ('record-1', 'message_id_1', 'debit', 100.0, 
        datetime.datetime(2024, 1, 1, 8, 0, 5, tzinfo=datetime.timezone.utc), 
        'Sample Description', 
        'Entertainment',
        datetime.datetime(2024, 1, 2, 8, 0, 12, tzinfo=datetime.timezone.utc)),
        
    ]
