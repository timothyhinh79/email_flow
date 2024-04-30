from google.oauth2.credentials import Credentials
from apiclient import discovery
import base64
import re
import functions_framework

from models.financial_transaction import FinancialTransaction
from classes.db_credentials import DBCredentials
from db_utils.db_functions import update_record
from lib.google_drive.google_drive import delete_file
from lib.gcp.gcp import access_secret_version

@functions_framework.cloud_event
def process_categorization_submission(cloud_event):

    print("Pubsub Message")
    print(cloud_event.data["message"])

    ##### Load all necessary credentials

    # Setting up credentials for Google Forms API
    google_forms_api_secrets = access_secret_version('email-parser-414818', 'google_forms_api_credentials', '1')
    google_forms_creds = Credentials.from_authorized_user_info({
        'client_id': google_forms_api_secrets['client_id'], 
        'client_secret': google_forms_api_secrets['client_secret'],
        'refresh_token': google_forms_api_secrets['refresh_token']
    })

    # Setting up Google Drive API credentials
    google_drive_api_secrets = access_secret_version('email-parser-414818', 'google_drive_api_credentials', '1')
    google_drive_creds = Credentials.from_authorized_user_info({
        'client_id': google_drive_api_secrets['client_id'], 
        'client_secret': google_drive_api_secrets['client_secret'],
        'refresh_token': google_drive_api_secrets['refresh_token']
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

    ##### Process Google Form submission

    form_id = cloud_event.data["message"]["attributes"]["formId"]

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
        id=record_id,
        field='category',
        new_value=latest_answer_value
    )
    print(f"Updated category on record with id {record_id}")

    # Delete the google form
    delete_file(
        google_creds=google_drive_creds,
        file_id=form_id
    )
    print(f"Deleted form with id {form_id}")

    print("Done")
