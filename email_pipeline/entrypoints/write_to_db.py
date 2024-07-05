import os
import json
import logging
import base64
from flask import Flask, request

from confluent_kafka import Consumer, KafkaException
from google.oauth2.credentials import Credentials

from src.services.gcp.gcp import access_secret_version
from src.entities.db_credentials import DBCredentials
from src.database.models.financial_transaction import FinancialTransaction
from src.database.operations.db_functions import insert_record, upsert_record
from src.services.gmail.gmail import compose_and_send_email
from src.services.google_forms.google_forms import (
    create_google_form, 
    create_google_form_watch,
)
from src.services.google_forms.questions.categorize_transaction_question import (
    generate_transaction_categorization_question,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting index.py")

# Get environment variables
DB_CREDS_SECRET = os.getenv('DB_CREDS_SECRET')
DB_CREDS_SECRET_VER = os.getenv('DB_CREDS_SECRET_VER')
CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET')
CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET_VER = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET_VER')
CLASSIFIED_TRANSACTIONS_TOPIC = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC')
CLASSIFIED_TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID = os.getenv('CLASSIFIED_TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID')
TOPIC_AUTO_OFFSET_RESET = os.getenv('TOPIC_AUTO_OFFSET_RESET')
TOPIC_SECURITY_PROTOCOL = os.getenv('TOPIC_SECURITY_PROTOCOL')
TOPIC_SASL_MECHANISMS = os.getenv('TOPIC_SASL_MECHANISMS')
DUMMY_GMAIL_API_CREDS_SECRET = os.getenv('DUMMY_GMAIL_API_CREDS_SECRET')
DUMMY_GMAIL_API_CREDS_SECRET_VER = os.getenv('DUMMY_GMAIL_API_CREDS_SECRET_VER')
GOOGLE_FORMS_API_CREDS_SECRET = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET')
GOOGLE_FORMS_API_CREDS_SECRET_VER = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET_VER')
DUMMY_GMAIL = os.getenv('DUMMY_GMAIL')
MAIN_GMAIL = os.getenv('MAIN_GMAIL')
CATEGORIZATION_SUBMISSION_TOPIC = os.getenv('CATEGORIZATION_SUBMISSION_TOPIC')
CATEGORIZATION_GOOGLE_FORM_TITLE = os.getenv('CATEGORIZATION_GOOGLE_FORM_TITLE')

logger.info("Environment variables loaded")

# Get credentials to read from classified-transactions topic from Secret Manager
classified_transactions_topic_read_creds = access_secret_version(
    'email-parser-414818', 
    CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET, 
    CLASSIFIED_TRANSACTIONS_TOPIC_READ_CREDS_SECRET_VER,
)

# Setting up credentials for Google Forms API
google_forms_api_secrets = access_secret_version(
    'email-parser-414818', 
    GOOGLE_FORMS_API_CREDS_SECRET, 
    GOOGLE_FORMS_API_CREDS_SECRET_VER
)

# Setting up credentials for the dummy gmail account
dummy_gmail_api_secrets = access_secret_version(
    'email-parser-414818', 
    DUMMY_GMAIL_API_CREDS_SECRET, 
    DUMMY_GMAIL_API_CREDS_SECRET_VER
)

# Access the DB credentials from Secret Manager
db_creds_json = access_secret_version(
    'email-parser-414818', 
    DB_CREDS_SECRET, 
    DB_CREDS_SECRET_VER,
)

logger.info("Credentials loaded")

# Create DBCredentials object
db_creds = DBCredentials(
    host = db_creds_json['DB_HOST'],
    port = db_creds_json['DB_PORT'],
    user = db_creds_json['DB_USER'],
    password = db_creds_json['DB_PASSWORD'],
    database = db_creds_json['DB_DATABASE'],
)

# Define the Kafka Consumer configuration
kafka_consumer_config = {
    'bootstrap.servers': classified_transactions_topic_read_creds['bootstrap.servers'],
    'group.id': CLASSIFIED_TRANSACTIONS_TOPIC_CONSUMER_GROUP_ID,
    'auto.offset.reset': TOPIC_AUTO_OFFSET_RESET,
    'security.protocol': TOPIC_SECURITY_PROTOCOL,
    'sasl.mechanisms': TOPIC_SASL_MECHANISMS,
    'sasl.username': classified_transactions_topic_read_creds['sasl.username'],
    'sasl.password': classified_transactions_topic_read_creds['sasl.password']
}

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():

    ##### Parse the Pub/Sub message
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        logger.error(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        logger.error(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    pubsub_message_data = base64.b64decode(pubsub_message["data"])
    print("Pubsub Message")
    print(pubsub_message_data)

    # Subscribe to the Kafka topic
    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([CLASSIFIED_TRANSACTIONS_TOPIC])

    try:

        while True:
            
            # Poll for messages
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:

                # Load the JSON message from topic
                data_json = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message from topic: {msg.value().decode('utf-8')}")

                # Insert or upsert classified transaction into DB table
                if data_json['pipeline_source'] == 'parse_email':
                    res = insert_record(FinancialTransaction, data_json, db_creds)
                else:
                    res = upsert_record(FinancialTransaction, data_json, db_creds)

                # Log the result of the insert/upsert operation
                if res == {'status': 'updated'}:
                    logger.info(f"Upserted message into DB: {msg.value().decode('utf-8')}.")

                elif res == {'status': 'failed to insert duplicate record'}:
                    logger.info(f"Failed to insert duplicate record into DB: {msg.value().decode('utf-8')}")
                    
                elif res == {'status': 'inserted'}:
                    logger.info(f"Inserted message to DB: {msg.value().decode('utf-8')}")

                    # If this is a new transaction from an automatically parsed + classified email, 
                        # send an email # to the main account asking for confirmation or correction 
                        # of the transaction category
                    if data_json['pipeline_source'] == 'parse_email':
                        
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
                        
                        # Create google form for categorizing the transaction
                        google_forms_creds = Credentials.from_authorized_user_info({
                            'client_id': google_forms_api_secrets['client_id'], 
                            'client_secret': google_forms_api_secrets['client_secret'],
                            'refresh_token': google_forms_api_secrets['refresh_token']
                        })
                        google_form = create_google_form(
                            google_creds=google_forms_creds, 
                            google_form_title=CATEGORIZATION_GOOGLE_FORM_TITLE,
                            google_form_document_title=subject,
                            google_form_questions=question
                        )

                        logger.info(f"Created google form with ID: {google_form['formId']}")

                        # Set up a watch on the google form so that submissions trigger PubSub topic
                        gf_watch = create_google_form_watch(
                            google_creds=google_forms_creds,
                            form_id=google_form['formId'],
                            event_type='RESPONSES',
                            topic_name=CATEGORIZATION_SUBMISSION_TOPIC
                        )

                        logger.info(f"Set up watch on google form with the following response: {gf_watch}")

                        # Compose email with link to google form, and send to main email account
                        dummy_gmail_creds = Credentials.from_authorized_user_info({
                            'client_id': dummy_gmail_api_secrets['client_id'], 
                            'client_secret': dummy_gmail_api_secrets['client_secret'],
                            'refresh_token': dummy_gmail_api_secrets['refresh_token']
                        })

                        body = f"The transaction \"{data_json['description']}\" was automatically categorized as \"{data_json['category_ml']}\".\n\n"
                        body += f"If this is inaccurate, please visit the following link to reassign the appropriate category: \n"
                        body += f"https://docs.google.com/forms/d/{google_form['formId']}/viewform?edit_requested=true"
                        
                        send_email_res = compose_and_send_email(
                            google_creds=dummy_gmail_creds,
                            sender=DUMMY_GMAIL, # dummy account
                            to=MAIN_GMAIL,
                            subject=subject,
                            body=body
                        )

                        logger.info(f"Sent email with the following message: {send_email_res}")
                
                else:
                    logger.error(f"An unexpected error occurred while saving message to DB: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
