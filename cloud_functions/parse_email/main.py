from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import base64
import functions_framework

from models.message_id import MessageIDs
from models.financial_transaction import FinancialTransaction
from classes.db_credentials import DBCredentials
from db_utils.db_functions import save_to_db
from lib.parse_helpers import (
    access_secret_version,
    get_messages_after_specific_message, 
    process_message,
    get_latest_message_id
)
from lib.gmail.gmail import compose_and_send_email
from lib.google_forms.common import create_google_form, create_google_form_watch
from lib.google_forms.transaction_categorization.categorize_transaction_question import generate_transaction_categorization_question

@functions_framework.cloud_event
def parse_data_and_save_to_db(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    ##### Load all necessary credentials

    # Setting up Gmail API credentials for both the main and dummy gmail accounts
    main_gmail_api_secrets = access_secret_version('email-parser-414818', 'gmail_api_credentials', '7')
    main_gmail_creds = Credentials.from_authorized_user_info({
        'client_id': main_gmail_api_secrets['client_id'], 
        'client_secret': main_gmail_api_secrets['client_secret'],
        'refresh_token': main_gmail_api_secrets['refresh_token']
    })

    dummy_gmail_api_secrets = access_secret_version('email-parser-414818', 'dummy_gmail_api_credentials', '1')
    dummy_gmail_creds = Credentials.from_authorized_user_info({
        'client_id': dummy_gmail_api_secrets['client_id'], 
        'client_secret': dummy_gmail_api_secrets['client_secret'],
        'refresh_token': dummy_gmail_api_secrets['refresh_token']
    })

    # Setting up credentials for Google Forms API
    google_forms_api_secrets = access_secret_version('email-parser-414818', 'google_forms_api_credentials', '1')
    google_forms_creds = Credentials.from_authorized_user_info({
        'client_id': google_forms_api_secrets['client_id'], 
        'client_secret': google_forms_api_secrets['client_secret'],
        'refresh_token': google_forms_api_secrets['refresh_token']
    })

    # Set up DB credentials
    db_creds_json = access_secret_version('email-parser-414818', 'gmail_logger_postgres_credentials', '1')
    db_creds = DBCredentials(
        host = db_creds_json['DB_HOST'],
        port = db_creds_json['DB_PORT'],
        user = db_creds_json['DB_USER'],
        password = db_creds_json['DB_PASSWORD'],
        database = db_creds_json['DB_DATABASE'],
    )

    ##### Identify the latest transaction emails and save data to DB

    gmail = build('gmail', 'v1', credentials=main_gmail_creds)

    # Specify the start messageId
    start_message_id = MessageIDs.fetch_latest_messageid(db_creds)

    if not start_message_id:
        start_message_id = None

    print(f'Previous message ID: {start_message_id}')

    messages = get_messages_after_specific_message(gmail, start_message_id, label_ids=['Label_3935809748622434433'])

    if messages:
        for message in messages:

            # Process each transaction email and save relevant data to DB
            data_json = process_message(gmail, message['id'], save_to_db_= False, db_creds = db_creds)

            save_to_db_result = False
            if data_json:
                save_to_db_result = save_to_db(FinancialTransaction, data_json, db_creds)

            # if record was not saved to database (because the record already existed or 
            #   because the message was not meant to be parsed), do no create google form
            if not save_to_db_result:
                continue

            # Create google form for categorizing the transaction
            subject = f"Categorize \"{data_json['description']}\" Transaction"
            question = generate_transaction_categorization_question(
                record_id=data_json['id'],
                message_id=data_json['message_id'],
                transaction_type=data_json['transaction_type'],
                transaction_date=data_json['transaction_date'],
                description=data_json['description'],
                amount=data_json['amount']
            )

            # Set up a watch on the google form so that submissions trigger PubSub topic
            google_form = create_google_form(
                google_creds=google_forms_creds, 
                google_form_title='Categorize Financial Transaction',
                google_form_document_title=subject,
                google_form_questions=question
            )

            create_google_form_watch(
                google_creds=google_forms_creds,
                form_id=google_form['formId'],
                event_type='RESPONSES',
                topic_name='projects/email-parser-414818/topics/categorize-transactions-form-submissions'
            )

            # Compose email with link to google form, and send to main email account
            body = f"Please visit the following link: \nhttps://docs.google.com/forms/d/{google_form['formId']}/viewform?edit_requested=true"
            compose_and_send_email(
                google_creds=dummy_gmail_creds,
                sender='tim098292@gmail.com', # dummy account
                to='timothyhinh79@gmail.com',
                subject=subject,
                body=body
            )

        # Save the id of the latest message - this will be used in subsequent runs to quickly identify new transaction emails
        latest_message_id = get_latest_message_id(gmail, messages)
        MessageIDs.add_messageid(latest_message_id, db_creds)

    print('Done')
