from lib.google_forms.google_forms import (
    create_google_form,
)
from lib.google_forms.questions.categorize_transaction_question import generate_transaction_categorization_question
from lib.google_drive.google_drive import delete_file

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials

import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_FORM_CLIENT_ID = os.getenv('GOOGLE_FORM_CLIENT_ID')
GOOGLE_FORM_CLIENT_SECRET = os.getenv('GOOGLE_FORM_CLIENT_SECRET')
GOOGLE_FORM_REFRESH_TOKEN = os.getenv('GOOGLE_FORM_REFRESH_TOKEN')
GOOGLE_DRIVE_CLIENT_ID = os.getenv('GOOGLE_DRIVE_CLIENT_ID')
GOOGLE_DRIVE_CLIENT_SECRET = os.getenv('GOOGLE_DRIVE_CLIENT_SECRET')
GOOGLE_DRIVE_REFRESH_TOKEN = os.getenv('GOOGLE_DRIVE_REFRESH_TOKEN')

def test_delete_file():
    question = generate_transaction_categorization_question(
        record_id='record-1',
        message_id='message-1',
        transaction_type='credit',
        transaction_date='April 1st, 2024',
        description='Rent',
        amount=1000.0
    )

    # Create credentials
    form_creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_FORM_CLIENT_ID, 
        'client_secret': GOOGLE_FORM_CLIENT_SECRET,
        'refresh_token': GOOGLE_FORM_REFRESH_TOKEN
    })

    form_result = create_google_form(
        google_creds=form_creds,
        google_form_title='Sample Form for Pytest',
        google_form_document_title='Categorize Financial Transaction',
        google_form_questions=question
    )

    drive_creds = Credentials.from_authorized_user_info({
        'client_id': GOOGLE_DRIVE_CLIENT_ID, 
        'client_secret': GOOGLE_DRIVE_CLIENT_SECRET,
        'refresh_token': GOOGLE_DRIVE_REFRESH_TOKEN
    })

    delete_file(google_creds=drive_creds, file_id = form_result['formId'])

    # Check if file still exists
    drive_service = build('drive', 'v3', credentials=drive_creds)
    try:
        file = drive_service.files().get(fileId='1rhrxvgAvLIttaUXVGnlo_XBtxIJ-Vct6kwRLMdNkZ_8').execute()
        assert False, f"File with id {form_result['formId']} was not deleted."
    except:
        assert True
