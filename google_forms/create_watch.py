from google.oauth2.credentials import Credentials
from apiclient import discovery
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_FORM_CLIENT_ID = os.getenv('GOOGLE_FORM_CLIENT_ID')
GOOGLE_FORM_CLIENT_SECRET = os.getenv('GOOGLE_FORM_CLIENT_SECRET')
GOOGLE_FORM_REFRESH_TOKEN = os.getenv('GOOGLE_FORM_REFRESH_TOKEN')

# Create credentials
google_creds = Credentials.from_authorized_user_info({
    'client_id': GOOGLE_FORM_CLIENT_ID, 
    'client_secret': GOOGLE_FORM_CLIENT_SECRET,
    'refresh_token': GOOGLE_FORM_REFRESH_TOKEN
})

form_id='1u0iDKQkxKn8Nj_QwsfjeZoXkfx_3JtRYJVuFiweuIds'
event_type='RESPONSES'
topic_name='projects/email-parser-414818/topics/log-transaction-form-submissions'


request = { 
    "watch": {
        "eventType": event_type, # Required. Which event type to watch for.
        "target": {
            "topic": {
                "topicName": topic_name, # Required. A fully qualified Pub/Sub topic name to publish the events to. This topic must be owned by the calling project and already exist in Pub/Sub.
            },
        },
    },
}

DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

form_service = discovery.build(
    "forms",
    "v1",
    credentials=google_creds,
    discoveryServiceUrl=DISCOVERY_DOC,
    static_discovery=False,
)

# Creates the initial form
result = form_service.forms().watches().create(formId=form_id, body=request).execute()