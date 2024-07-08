import re
import logging
import os
from flask import Flask, request

from google.oauth2.credentials import Credentials
from apiclient import discovery

from src.database.models.financial_transaction import FinancialTransaction
from src.entities.db_credentials import DBCredentials
from src.database.operations.db_functions import update_record
from src.services.google_drive.google_drive import delete_file
from src.services.gcp.gcp import access_secret_version

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting index.py")

def retrieve_google_form_answer(answers_dict, question_id):
    return answers_dict[question_id]['textAnswers']['answers'][0]['value']

# Loading environment variables
GOOGLE_FORMS_API_CREDS_SECRET = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET')
GOOGLE_FORMS_API_CREDS_SECRET_VER = os.getenv('GOOGLE_FORMS_API_CREDS_SECRET_VER')
GOOGLE_DRIVE_API_CREDS_SECRET = os.getenv('GOOGLE_DRIVE_API_CREDS_SECRET')
GOOGLE_DRIVE_API_CREDS_SECRET_VER = os.getenv('GOOGLE_DRIVE_API_CREDS_SECRET_VER')
DB_CREDS_SECRET = os.getenv('DB_CREDS_SECRET')
DB_CREDS_SECRET_VER = os.getenv('DB_CREDS_SECRET_VER')

logger.info("Environment variables loaded")

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():

    try:

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

        # Setting up Google Drive API credentials
        google_drive_api_secrets = access_secret_version(
            'email-parser-414818', 
            GOOGLE_DRIVE_API_CREDS_SECRET, 
            GOOGLE_DRIVE_API_CREDS_SECRET_VER
        )
        google_drive_creds = Credentials.from_authorized_user_info({
            'client_id': google_drive_api_secrets['client_id'], 
            'client_secret': google_drive_api_secrets['client_secret'],
            'refresh_token': google_drive_api_secrets['refresh_token']
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
        latest_answer_value = list(answers_dict.values())[0]['textAnswers']['answers'][0]['value']

        # Extract details on message_id and record id from form title
        form = form_service.forms().get(formId=form_id).execute()
        form_title = form['items'][0]['title']
        record_id = re.search('Record ID: \"(.*?)\"', form_title).group(1)

        # Update the category on the FinancialTransaction record in DB
        update_record(
            model=FinancialTransaction,
            db_creds=db_creds,
            data_json={
                'id': record_id,
                'category': latest_answer_value,
                'pipeline_source': 'process_transaction_categorization_form_submission'
            }
        )
        logger.info(f"Updated category on record with id {record_id}")

        # Delete the google form
        delete_file(
            google_creds=google_drive_creds,
            file_id=form_id
        )
        logger.info(f"Deleted form with id {form_id}")

        logger.info(f"Finished processing form submission")

    except Exception as e:
        logger.error(f"error: {e}")

    # always return a 204 to prevent Cloud Run service from infinitely retrying the request in case of an error
    finally:
        return ('', 204)
