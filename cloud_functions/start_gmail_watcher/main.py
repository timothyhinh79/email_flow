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
def start_gmail_watcher(cloud_event):

    pubsub_msg = base64.b64decode(cloud_event.data["message"]["data"])
    print("Pubsub Message")
    print(pubsub_msg)

    api_secrets = access_secret_version('email-parser-414818', 'gmail_api_credentials', '6')

    # Create credentials
    creds = Credentials.from_authorized_user_info({
        'client_id': api_secrets['client_id'], 
        'client_secret': api_secrets['client_secret'],
        'refresh_token': api_secrets['refresh_token']
    })

    gmail = build('gmail', 'v1', credentials=creds)

    # Start Gmail watcher so that "personal-emails" PubSub topic receives a message for incoming emails assigned to given labels 
    request = {
      'labelIds': ['Label_3884943773140766149', 'Label_3935809748622434433' ],
      'topicName': 'projects/email-parser-414818/topics/personal-emails',
      'labelFilterBehavior': 'INCLUDE',
      'historyTypes': ['messageAdded']
    }

    resp = gmail.users().watch(userId='me', body=request).execute()
    print("Successfully started watch request.")