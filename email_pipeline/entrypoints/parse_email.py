
import base64
import os
import json
import logging
from flask import Flask, request

from confluent_kafka import Producer
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from src.database.models.message_id import MessageIDs
from src.entities.db_credentials import DBCredentials
from src.email_processing.parsers import (
    process_financial_transaction_message,
)
from src.services.gcp.gcp import access_secret_version
from src.services.gmail.gmail import (
    get_messages_after_specific_message,
    get_latest_message_id,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting index.py")

# Loading environment variables
MAIN_GMAIL_API_CREDS_SECRET = os.getenv('MAIN_GMAIL_API_CREDS_SECRET')
MAIN_GMAIL_API_CREDS_SECRET_VER = os.getenv('MAIN_GMAIL_API_CREDS_SECRET_VER')
DB_CREDS_SECRET = os.getenv('DB_CREDS_SECRET')
DB_CREDS_SECRET_VER = os.getenv('DB_CREDS_SECRET_VER')
GMAIL_LABELS = os.getenv('GMAIL_LABELS')
TRANSACTIONS_TOPIC_CREDS_SECRET = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET')
TRANSACTIONS_TOPIC_CREDS_SECRET_VER = os.getenv('TRANSACTIONS_TOPIC_CREDS_SECRET_VER')
TRANSACTIONS_TOPIC = os.getenv('TRANSACTIONS_TOPIC')

logger.info("Environment variables loaded")

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

    ##### Load all necessary credentials
    # Setting up Gmail API credentials for both the main and dummy gmail accounts
    main_gmail_api_secrets = access_secret_version('email-parser-414818', MAIN_GMAIL_API_CREDS_SECRET, MAIN_GMAIL_API_CREDS_SECRET_VER)
    main_gmail_creds = Credentials.from_authorized_user_info({
        'client_id': main_gmail_api_secrets['client_id'], 
        'client_secret': main_gmail_api_secrets['client_secret'],
        'refresh_token': main_gmail_api_secrets['refresh_token']
    })

    # Set up DB credentials
    db_creds_json = access_secret_version('email-parser-414818', DB_CREDS_SECRET, DB_CREDS_SECRET_VER)
    db_creds = DBCredentials(
        host = db_creds_json['DB_HOST'],
        port = db_creds_json['DB_PORT'],
        user = db_creds_json['DB_USER'],
        password = db_creds_json['DB_PASSWORD'],
        database = db_creds_json['DB_DATABASE'],
    )

    # Grab transactions Kafka topic credentials
    producer_config = access_secret_version('email-parser-414818', TRANSACTIONS_TOPIC_CREDS_SECRET, TRANSACTIONS_TOPIC_CREDS_SECRET_VER)


    ##### Identify the latest transaction emails since the previous run
    gmail = build('gmail', 'v1', credentials=main_gmail_creds)

    # Specify the start messageId
    start_message_id = MessageIDs.fetch_latest_messageid(db_creds)

    if not start_message_id:
        start_message_id = None

    logger.info(f'Fetching messages after the following message ID: {start_message_id}')

    # Convert string of labels to list
    label_ids = GMAIL_LABELS.split(',')
    messages = get_messages_after_specific_message(gmail, start_message_id, label_ids=label_ids)


    ##### Process each transaction email and push relevant data to Kafka topic
    if messages:

        for message in messages:

            # Process each transaction email and save relevant data to DB
            data_json = process_financial_transaction_message(gmail, message['id'], save_to_db_= False, db_creds = db_creds)
            data_json['pipeline_source'] = 'parse_email'  

            # Produce the message to the Kafka transactions topic
            producer = Producer(producer_config)
            producer.produce(TRANSACTIONS_TOPIC, value=json.dumps(data_json))
            producer.flush()

            logger.info(f'Transaction email processed successfully for message ID: {message["id"]}')

        # Save the id of the latest message - this will be used in subsequent runs to quickly identify new transaction emails
        latest_message_id = get_latest_message_id(gmail, messages)
        MessageIDs.add_messageid(latest_message_id, db_creds)

    logger.info('All messages processed successfully')

    return ('', 204)
