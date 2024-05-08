from google.oauth2.credentials import Credentials
from apiclient import discovery
import uuid
import datetime
import functions_framework

from models.financial_transaction import FinancialTransaction
from classes.db_credentials import DBCredentials
from db_utils.db_functions import save_to_db
from lib.gcp.gcp import access_secret_version


def retrieve_google_form_answer(answers_dict, question_id):
    return answers_dict[question_id]['textAnswers']['answers'][0]['value']

@functions_framework.cloud_event
def process_log_transaction_submission(cloud_event):

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

    # Extract the values for each field of the Financial Transactions table
    transaction_type = retrieve_google_form_answer(answers_dict, '138f7349') 
    amount = retrieve_google_form_answer(answers_dict, '18167d39') 
    transaction_date = retrieve_google_form_answer(answers_dict, '7bfcc13c')
    category = retrieve_google_form_answer(answers_dict, '2b40e6a8') 
    description = retrieve_google_form_answer(answers_dict, '7cc2fb41')

    # Set up data_json and save to database
    record_id = uuid.uuid4()
    data_json = {
        'id': record_id,
        'message_id': None,
        'transaction_type': transaction_type,
        'amount': amount,
        'transaction_date': transaction_date,
        'category': category,
        'description': description,
        'updated_at': datetime.datetime.now(datetime.timezone.utc)
    }

    save_to_db(
        model=FinancialTransaction, 
        data_json=data_json,
        db_creds=db_creds,
    )

    print(f"Added record with id {record_id}")

    print("Done")
