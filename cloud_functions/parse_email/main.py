from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import storage
import tensorflow as tf
import base64
import os
import functions_framework
from confluent_kafka import Producer
import json

from models.message_id import MessageIDs
from models.financial_transaction import FinancialTransaction
from classes.db_credentials import DBCredentials
from db_utils.db_functions import save_to_db
from lib.parsers import (
    process_financial_transaction_message,
)
from lib.gcp.gcp import access_secret_version
from lib.gmail.gmail import (
    compose_and_send_email,
    get_messages_after_specific_message,
    get_latest_message_id,
)
from lib.google_forms.google_forms import (
    create_google_form, 
    create_google_form_watch,
)
from lib.google_forms.questions.categorize_transaction_question import (
    generate_transaction_categorization_question,
)

categories_text = [
    'Food', 'Personal & Miscellaneous', 'Savings & Investments',
    'Entertainment', 'Education', 'Living Expenses'
]

# Loading environment variables
main_gmail_api_creds_secret = os.getenv('MAIN_GMAIL_API_CREDS_SECRET')
main_gmail_api_creds_secret_ver = os.getenv('MAIN_GMAIL_API_CREDS_SECRET_VER')
dummy_gmail_api_creds_secret = os.getenv('DUMMY_GMAIL_API_CREDS_SECRET')
dummy_gmail_api_creds_secret_ver = os.getenv('DUMMY_GMAIL_API_CREDS_SECRET_VER')
google_forms_api_creds_secret = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET')
google_forms_api_creds_secret_ver = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET_VER')
db_creds_secret = os.getenv('DB_CREDS_SECRET')
db_creds_secret_ver = os.getenv('DB_CREDS_SECRET_VER')
gmail_labels = os.getenv('GMAIL_LABELS')
classification_model_bucket = os.getenv('CLASSIFICATION_MODEL_BUCKET')
model_file = os.getenv('MODEL_FILE')
categorization_submission_topic = os.getenv('CATEGORIZATION_SUBMISSION_TOPIC')
dummy_gmail = os.getenv('DUMMY_GMAIL')
main_gmail = os.getenv('MAIN_GMAIL')
categorization_google_form_title = os.getenv('CATEGORIZATION_GOOGLE_FORM_TITLE')
transactions_topic_creds_secret = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET')
transactions_topic_creds_secret_ver = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET_VER')

@functions_framework.cloud_event
def parse_data_and_save_to_db(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    ##### Load all necessary credentials

    # Setting up Gmail API credentials for both the main and dummy gmail accounts
    main_gmail_api_secrets = access_secret_version('email-parser-414818', main_gmail_api_creds_secret, main_gmail_api_creds_secret_ver)
    main_gmail_creds = Credentials.from_authorized_user_info({
        'client_id': main_gmail_api_secrets['client_id'], 
        'client_secret': main_gmail_api_secrets['client_secret'],
        'refresh_token': main_gmail_api_secrets['refresh_token']
    })

    dummy_gmail_api_secrets = access_secret_version('email-parser-414818', dummy_gmail_api_creds_secret, dummy_gmail_api_creds_secret_ver)
    dummy_gmail_creds = Credentials.from_authorized_user_info({
        'client_id': dummy_gmail_api_secrets['client_id'], 
        'client_secret': dummy_gmail_api_secrets['client_secret'],
        'refresh_token': dummy_gmail_api_secrets['refresh_token']
    })

    # Setting up credentials for Google Forms API
    google_forms_api_secrets = access_secret_version('email-parser-414818', google_forms_api_creds_secret, google_forms_api_creds_secret_ver)
    google_forms_creds = Credentials.from_authorized_user_info({
        'client_id': google_forms_api_secrets['client_id'], 
        'client_secret': google_forms_api_secrets['client_secret'],
        'refresh_token': google_forms_api_secrets['refresh_token']
    })

    # Set up DB credentials
    db_creds_json = access_secret_version('email-parser-414818', db_creds_secret, db_creds_secret_ver)
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

    # Convert string of labels to list
    label_ids = gmail_labels.split(',')
    messages = get_messages_after_specific_message(gmail, start_message_id, label_ids=label_ids)

    if messages:

        # Load classification model
        gcs = storage.Client()
        bucket = gcs.get_bucket(classification_model_bucket)
        blob = bucket.blob(model_file)
        blob.download_to_filename(model_file) 
        model = tf.keras.models.load_model(model_file)

        for message in messages:

            # Process each transaction email and save relevant data to DB
            data_json = process_financial_transaction_message(gmail, message['id'], save_to_db_= False, db_creds = db_creds)

            save_to_db_result = False
            if data_json:
                
                # Predict category based on description
                predicted_category = categories_text[model.predict([data_json['description']])[0].argmax(axis=-1)]
                data_json['category_ml'] = predicted_category
                data_json['category'] = predicted_category

                # Save data to DB
                save_to_db_result = save_to_db(FinancialTransaction, data_json, db_creds)

            # if record was not saved to database (because the record already existed or 
            #   because the message was not meant to be parsed), do not create google form
            if not save_to_db_result:
                continue

            # Grab transactions Kafka topic credentials
            producer_config = access_secret_version('email-parser-414818', transactions_topic_creds_secret, transactions_topic_creds_secret_ver)

            # Create a Kafka producer
            producer = Producer(producer_config)

            # Define the topic
            topic = 'transactions'

            # Produce the message to the Kafka topic
            producer.produce(topic, value=json.dumps(data_json))

            # Wait for all messages to be sent
            producer.flush()

            # Create google form for categorizing the transaction
            subject = f"Categorize \"{data_json['description']}\" Transaction"
            question = generate_transaction_categorization_question(
                record_id=data_json['id'],
                message_id=data_json['message_id'],
                transaction_type=data_json['transaction_type'],
                transaction_date=data_json['transaction_date'],
                description=data_json['description'],
                amount=data_json['amount'],
                category_ml=data_json['category_ml']
            )

            google_form = create_google_form(
                google_creds=google_forms_creds, 
                google_form_title=categorization_google_form_title,
                google_form_document_title=subject,
                google_form_questions=question
            )

            # Set up a watch on the google form so that submissions trigger PubSub topic
            create_google_form_watch(
                google_creds=google_forms_creds,
                form_id=google_form['formId'],
                event_type='RESPONSES',
                topic_name=categorization_submission_topic
            )

            # Compose email with link to google form, and send to main email account
            body = f"The transaction \"{data_json['description']}\" was automatically categorized as \"{predicted_category}\".\n\n"
            body += f"If this is inaccurate, please visit the following link to reassign the appropriate category: \n"
            body += f"https://docs.google.com/forms/d/{google_form['formId']}/viewform?edit_requested=true"
            compose_and_send_email(
                google_creds=dummy_gmail_creds,
                sender=dummy_gmail, # dummy account
                to=main_gmail,
                subject=subject,
                body=body
            )

        # Save the id of the latest message - this will be used in subsequent runs to quickly identify new transaction emails
        latest_message_id = get_latest_message_id(gmail, messages)
        MessageIDs.add_messageid(latest_message_id, db_creds)

    print('Done')
