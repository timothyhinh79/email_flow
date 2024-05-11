from google.cloud import secretmanager
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import functions_framework
import json
import base64

def access_secret_version(project_id, secret_id, version_id, service_account_file = None):

    # Create Secret Manager client.
    if service_account_file:
        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        client = secretmanager.SecretManagerServiceClient(credentials = credentials)
    else:
        client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Return the decoded payload.
    return json.loads(response.payload.data.decode('UTF-8'))

@functions_framework.cloud_event
def renew_google_form_watch(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    # Setting up credentials for Google Forms API
    api_secrets = access_secret_version('email-parser-414818', 'google_forms_api_credentials', '1')
    creds = Credentials.from_authorized_user_info({
        'client_id': api_secrets['client_id'], 
        'client_secret': api_secrets['client_secret'],
        'refresh_token': api_secrets['refresh_token']
    })

    # Renew watch on the Google Form with ID "1u0iDKQkxKn8Nj_QwsfjeZoXkfx_3JtRYJVuFiweuIds" so that form submissions trigger the "log-transaction-form-submissions" PubSub topic
    DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"
    form_service = build("forms", "v1", credentials=creds, discoveryServiceUrl=DISCOVERY_DOC, static_discovery=False)
    response = form_service.forms().watches().renew(formId="1u0iDKQkxKn8Nj_QwsfjeZoXkfx_3JtRYJVuFiweuIds", watchId="34a395e1-8d6e-4a4f-981e-4dcf07795d4c").execute()

    print(response)