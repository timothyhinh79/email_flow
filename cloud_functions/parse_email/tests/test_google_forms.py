from lib.google_forms.common import (
    create_google_form,
    create_google_form_watch,
)
from lib.google_forms.transaction_categorization.categorize_transaction_question import generate_transaction_categorization_question
from google.oauth2.credentials import Credentials
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_FORM_CLIENT_ID = os.getenv('GOOGLE_FORM_CLIENT_ID')
GOOGLE_FORM_CLIENT_SECRET = os.getenv('GOOGLE_FORM_CLIENT_SECRET')
GOOGLE_FORM_REFRESH_TOKEN = os.getenv('GOOGLE_FORM_REFRESH_TOKEN')

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
