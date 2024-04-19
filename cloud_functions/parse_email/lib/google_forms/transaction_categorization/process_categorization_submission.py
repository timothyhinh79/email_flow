from apiclient import discovery
import re

from db_utils.db_functions import update_record
from models.financial_transaction import FinancialTransaction

def process_categorization_submission(google_creds, db_creds, model, form_id):

    DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

    form_service = discovery.build(
        "forms",
        "v1",
        credentials=google_creds,
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

    update_record(
        model=model,
        db_creds=db_creds,
        id=record_id,
        field='category',
        new_value=latest_answer_value
    )

    return {
        'record_id': record_id,
        'category': latest_answer_value
    }