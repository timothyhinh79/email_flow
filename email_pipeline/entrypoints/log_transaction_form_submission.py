import uuid
import datetime
import json
import base64
import os
import logging
from flask import Flask, request

from confluent_kafka import Producer
from google.oauth2.credentials import Credentials
from apiclient import discovery

from src.services.gcp.gcp import access_secret_version

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting index.py")

def retrieve_google_form_answer(answers_dict, question_id):
    return answers_dict[question_id]['textAnswers']['answers'][0]['value']

# Loading environment variables
GOOGLE_FORMS_API_CREDS_SECRET = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET')
GOOGLE_FORMS_API_CREDS_SECRET_VER = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET_VER')
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

    print("Pubsub Message")
    print(pubsub_message)

    ##### Load all necessary credentials
    # Setting up credentials for Google Forms API
    google_forms_api_secrets = access_secret_version(
        'email-parser-414818', 
        GOOGLE_FORMS_API_CREDS_SECRET, 
        GOOGLE_FORMS_API_CREDS_SECRET_VER
    )
    google_forms_creds = Credentials.from_authorized_user_info({
        'client_id': google_forms_api_secrets['client_id'], 
        'client_secret': google_forms_api_secrets['client_secret'],
        'refresh_token': google_forms_api_secrets['refresh_token']
    })

    # Grab transactions Kafka topic credentials
    producer_config = access_secret_version(
        'email-parser-414818', 
        TRANSACTIONS_TOPIC_CREDS_SECRET, 
        TRANSACTIONS_TOPIC_CREDS_SECRET_VER
    )

    ##### Process Google Form submission
    form_id = pubsub_message["attributes"]["formId"]

    DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

    form_service = discovery.build(
        "forms",
        "v1",
        credentials=google_forms_creds,
        discoveryServiceUrl=DISCOVERY_DOC,
        static_discovery=False,
    )

    responses = form_service.forms().responses().list(formId=form_id).execute()

    # Sort the responses by 'lastSubmittedTime'
    sorted_responses = sorted(responses['responses'], key=lambda x: x['lastSubmittedTime'], reverse=True)

    # Extract the latest answer value
    answers_dict = sorted_responses[0]['answers']

    # Extract the values for each field of the Financial Transactions table
    transaction_type = retrieve_google_form_answer(answers_dict, '138f7349') 
    amount = retrieve_google_form_answer(answers_dict, '18167d39') 
    transaction_date = retrieve_google_form_answer(answers_dict, '7bfcc13c')
    category = retrieve_google_form_answer(answers_dict, '2b40e6a8') 
    description = retrieve_google_form_answer(answers_dict, '7cc2fb41')

    # Set up data_json and save to database
    record_id = str(uuid.uuid4())
    data_json = {
        'id': record_id,
        'message_id': None,
        'transaction_type': transaction_type,
        'amount': amount,
        'transaction_date': transaction_date,
        'category': category,
        'description': description,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

    # Produce the message to the Kafka transactions topic
    producer = Producer(producer_config)
    producer.produce(TRANSACTIONS_TOPIC, value=json.dumps(data_json))
    producer.flush()

    logger.info(f'Google Form submission processed successfully for transaction ID: {record_id}')

    return ('', 204)
